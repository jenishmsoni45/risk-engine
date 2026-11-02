"""The ledger stub's POST /transfers (design.md 1.3): tally's transfer path for CI.

The round trip runs the stub container against the real decision API (criterion 9). The rest run
the stub in this process against a fake decision API, which answers whatever a test needs
(criterion 10).
"""

from __future__ import annotations

import json
import threading
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import httpx
import pytest

from ledger_stub.transfers import ENTRY_FIELDS
from tests.fake_risk import FakeRisk, decided, raw
from tests.helpers import STUB_URL, TRACEPARENT, TopicTail, count, database, text_headers, unique
from tests.stub_process import running_stub

KEY_REUSED = "Idempotency-Key was already used for a different request"


def transfer(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "fromAccountId": str(uuid.uuid4()),
        "toAccountId": str(uuid.uuid4()),
        "amount": 500,
        "currency": "CAD",
    }
    body.update(overrides)
    return body


def post_transfer(base_url: str, key: str, body: dict[str, Any]) -> httpx.Response:
    return httpx.post(
        f"{base_url}/transfers", headers={"Idempotency-Key": key}, json=body, timeout=10
    )


def saved_keys(key: str) -> int:
    return count("select count(*) from stub.transfers where idempotency_key = %s", (key,))


def outbox_rows(body: dict[str, Any]) -> int:
    """Outbox rows for transfers out of this body's sender; every test uses new accounts."""
    sender = json.dumps({"lines": [{"accountId": body["fromAccountId"]}]})
    return count("select count(*) from stub.outbox where payload @> %s::jsonb", (sender,))


@pytest.fixture
def fake() -> Iterator[FakeRisk]:
    with FakeRisk() as risk:
        yield risk


@pytest.fixture
def stub(fake: FakeRisk) -> Iterator[str]:
    with running_stub(fake.url) as url:
        yield url


# --- Criterion 9: against the real decision API ---------------------------------------------


def test_allow_round_trip() -> None:
    tail = TopicTail()
    try:
        body = transfer(description="rent")
        response = post_transfer(STUB_URL, unique("round-trip"), body)
        assert response.status_code == 201, response.text
        entry = response.json()
        assert entry["riskCheck"] == "decided"
        message = tail.wait_for(entry["id"])
    finally:
        tail.close()

    event = json.loads(message.value() or b"")
    assert event == entry
    assert event["decisionId"] is not None
    # One header, a W3C trace context, as tally sends (D44).
    (header,) = text_headers(message)
    assert header[0] == "traceparent"
    assert TRACEPARENT.fullmatch(header[1])
    # The join of US9: the event's decisionId is a decision in the log, the one that allowed it.
    with database() as conn:
        row = conn.execute(
            "select decision, from_account_id, amount from decisions where decision_id = %s",
            (event["decisionId"],),
        ).fetchone()
    assert row is not None
    assert (row[0], str(row[1]), row[2]) == ("allow", body["fromAccountId"], body["amount"])


# --- Criterion 10: against a fake decision API ----------------------------------------------


@pytest.mark.parametrize(
    ("decision", "problem_type"),
    [("hold", "/problems/transfer-held"), ("block", "/problems/transfer-blocked")],
)
def test_a_refusal_gives_403_with_the_decision_id_and_no_event(
    fake: FakeRisk, stub: str, decision: str, problem_type: str
) -> None:
    decision_id = str(uuid.uuid4())
    fake.respond_with(decided(decision, decision_id=decision_id, fired=["velocity_1m"]))
    body, key = transfer(), unique("refused")
    response = post_transfer(stub, key, body)

    assert response.status_code == 403
    assert response.headers["content-type"] == "application/problem+json"
    problem = response.json()
    assert problem["type"] == problem_type
    assert problem["decisionId"] == decision_id
    # The fired rules stay with the decision API (D3).
    assert "velocity_1m" not in response.text
    assert outbox_rows(body) == 0
    assert saved_keys(key) == 0


def test_a_409_from_the_decision_api_gives_409(fake: FakeRisk, stub: str) -> None:
    fake.respond_with(raw(409, b'{"type":"about:blank","status":409}'))
    body, key = transfer(), unique("conflict")
    response = post_transfer(stub, key, body)
    assert response.status_code == 409
    assert response.json()["detail"] == KEY_REUSED
    assert outbox_rows(body) == 0
    assert saved_keys(key) == 0


def test_the_same_key_and_body_replay_without_asking(fake: FakeRisk, stub: str) -> None:
    body, key = transfer(description="rent"), unique("replay")
    first = post_transfer(stub, key, body)
    assert first.status_code == 201
    assert fake.calls == 1

    # Field order and an absent description against a null one don't make it another request.
    respelled = dict(reversed(list(body.items())))
    second = post_transfer(stub, key, respelled)
    assert second.status_code == 201
    assert second.json() == first.json()
    assert tuple(second.json()) == ENTRY_FIELDS
    assert fake.calls == 1
    assert outbox_rows(body) == 1


def test_the_same_key_with_another_body_gives_409(fake: FakeRisk, stub: str) -> None:
    body, key = transfer(), unique("reused")
    assert post_transfer(stub, key, body).status_code == 201
    response = post_transfer(stub, key, {**body, "amount": 501})
    assert response.status_code == 409
    assert response.json()["detail"] == KEY_REUSED
    assert fake.calls == 1
    assert outbox_rows(body) == 1


def test_a_refused_transfer_saves_no_key(fake: FakeRisk, stub: str) -> None:
    body, key = transfer(), unique("retry")
    fake.respond_with(decided("hold"))
    assert post_transfer(stub, key, body).status_code == 403
    assert saved_keys(key) == 0

    # The retry asks again, and this time the transfer is allowed and posted.
    fake.respond_with(decided("allow"))
    response = post_transfer(stub, key, body)
    assert response.status_code == 201
    assert fake.calls == 2
    assert saved_keys(key) == 1


def test_concurrent_first_requests_post_once(fake: FakeRisk, stub: str) -> None:
    """tally-integration.md 4.2: both ask, one posts, and the others replay its 201."""
    fake.respond_with(decided("allow", decision_id=str(uuid.uuid4())))
    body, key = transfer(), unique("concurrent")
    start = threading.Barrier(8)

    def send(_: int) -> httpx.Response:
        start.wait()
        return post_transfer(stub, key, body)

    with ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(pool.map(send, range(8)))
    assert [r.status_code for r in responses] == [201] * 8
    assert len({r.text for r in responses}) == 1
    assert outbox_rows(body) == 1


def test_a_database_failure_gives_503(fake: FakeRisk) -> None:
    unreachable = "host=127.0.0.1 port=1 user=nobody password=dummy dbname=nothing"
    with running_stub(fake.url, database_url=unreachable) as url:
        response = post_transfer(url, unique("no-db"), transfer())
    assert response.status_code == 503
    assert response.headers["content-type"] == "application/problem+json"
    assert fake.calls == 0


def test_an_invalid_transfer_gives_400_without_asking(fake: FakeRisk, stub: str) -> None:
    response = post_transfer(stub, unique("bad"), transfer(amount=0))
    assert response.status_code == 400
    assert response.headers["content-type"] == "application/problem+json"
    assert fake.calls == 0
    missing_key = httpx.post(f"{stub}/transfers", json=transfer(), timeout=5)
    assert missing_key.status_code == 400
