"""The stub's replay mode (design.md 1.3; D40u, D40v, D52g, D52h). Phase 6, criterion 3: replay
mode is active only with STUB_REPLAY=1; every transfer is posted with riskCheck: decided; a
transfer without a decision gets 503 and stops the replay; the normal-mode tests are unchanged
(tests/integration/test_stub.py and tests/invariants/test_i4_caller_never_hangs.py are not
touched by this phase).

The stub runs in this process against a fake decision API, as in test_stub.py, and publishes to
the stack's Kafka.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Iterator
from typing import Any

import httpx
import pytest

from ledger_stub.config import StubConfig
from ml.paysim import STEP_1_MS, Transfer
from ml.replay import ReplayError, replay
from tests.fake_risk import FakeRisk, Responder, decided, raw, silent
from tests.helpers import TopicTail, count, unique
from tests.stub_process import running_stub

REPLAY_TIME = "2026-01-01T03:00:00.250Z"


def transfer_body() -> dict[str, Any]:
    return {
        "fromAccountId": str(uuid.uuid4()),
        "toAccountId": str(uuid.uuid4()),
        "amount": 700,
        "currency": "CAD",
    }


def post(
    base_url: str, key: str, body: dict[str, Any], replay_time: str | None = REPLAY_TIME
) -> httpx.Response:
    headers = {"Idempotency-Key": key}
    if replay_time is not None:
        headers["X-Replay-Time"] = replay_time
    return httpx.post(f"{base_url}/transfers", headers=headers, json=body, timeout=10)


def saved_keys(key: str) -> int:
    return count("select count(*) from stub.transfers where idempotency_key = %s", (key,))


def outbox_rows(body: dict[str, Any]) -> int:
    sender = json.dumps({"lines": [{"accountId": body["fromAccountId"]}]})
    return count("select count(*) from stub.outbox where payload @> %s::jsonb", (sender,))


def first_then(first: Responder, then: Responder) -> Responder:
    """Answers the first request with `first` and every later one with `then`."""
    answered = 0

    async def respond(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> bool:
        nonlocal answered
        answered += 1
        return await (first if answered == 1 else then)(reader, writer)

    return respond


@pytest.fixture
def fake() -> Iterator[FakeRisk]:
    with FakeRisk() as risk:
        yield risk


@pytest.fixture
def replay_stub(fake: FakeRisk) -> Iterator[str]:
    with running_stub(fake.url, replay=True) as url:
        yield url


@pytest.fixture
def normal_stub(fake: FakeRisk) -> Iterator[str]:
    with running_stub(fake.url) as url:
        yield url


@pytest.mark.parametrize(("value", "on"), [(None, False), ("", False), ("0", False), ("1", True)])
def test_replay_mode_is_on_only_with_stub_replay_1(
    monkeypatch: pytest.MonkeyPatch, value: str | None, on: bool
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://dummy@db/dummy")
    if value is None:
        monkeypatch.delenv("STUB_REPLAY", raising=False)
    else:
        monkeypatch.setenv("STUB_REPLAY", value)
    assert StubConfig.from_env().replay is on


def test_any_other_stub_replay_value_stops_the_stub(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://dummy@db/dummy")
    monkeypatch.setenv("STUB_REPLAY", "true")
    with pytest.raises(RuntimeError, match="STUB_REPLAY"):
        StubConfig.from_env()


@pytest.mark.parametrize("decision", ["allow", "hold", "block"])
def test_every_decided_transfer_is_posted_at_its_replay_time(
    fake: FakeRisk, replay_stub: str, decision: str
) -> None:
    decision_id = str(uuid.uuid4())
    fake.respond_with(decided(decision, decision_id=decision_id))
    body, key = transfer_body(), unique("replay")
    tail = TopicTail()
    try:
        response = post(replay_stub, key, body)
        assert response.status_code == 201, response.text
        entry = response.json()
        message = tail.wait_for(entry["id"])
    finally:
        tail.close()

    assert entry["riskCheck"] == "decided"
    assert entry["decisionId"] == decision_id
    assert entry["createdAt"] == entry["effectiveAt"] == REPLAY_TIME
    # The decision was asked at the replay's time, not the wall clock's (D40v).
    assert fake.requests[-1]["requestedAt"] == REPLAY_TIME
    value = message.value()
    assert value is not None
    event = json.loads(value)
    assert (event["riskCheck"], event["decisionId"], event["createdAt"]) == (
        "decided",
        decision_id,
        REPLAY_TIME,
    )
    assert saved_keys(key) == 1


@pytest.mark.parametrize(
    "responder",
    [raw(500), raw(200, b"not json"), raw(200, b'{"decision": "maybe"}'), silent],
    ids=["500", "not-json", "bad-decision", "timeout"],
)
def test_a_transfer_without_a_decision_gets_503_and_is_not_posted(
    fake: FakeRisk, replay_stub: str, responder: Responder
) -> None:
    # The stub has heard onTimeout allow, so normal mode would post with fallback; replay mode
    # never posts without a decision.
    fake.respond_with(decided("allow", on_timeout="allow"))
    assert post(replay_stub, unique("replay"), transfer_body()).status_code == 201

    fake.respond_with(responder)
    body, key = transfer_body(), unique("replay")
    response = post(replay_stub, key, body)
    assert response.status_code == 503, response.text
    assert response.json()["type"] == "/problems/risk-unavailable"
    assert saved_keys(key) == 0
    assert outbox_rows(body) == 0


def test_a_409_from_the_decision_api_is_passed_on(fake: FakeRisk, replay_stub: str) -> None:
    fake.respond_with(raw(409))
    body, key = transfer_body(), unique("replay")
    assert post(replay_stub, key, body).status_code == 409
    assert saved_keys(key) == 0


@pytest.mark.parametrize("replay_time", [None, "yesterday", "2026-01-01T03:00:00"])
def test_a_missing_or_malformed_replay_time_gets_400_without_asking(
    fake: FakeRisk, replay_stub: str, replay_time: str | None
) -> None:
    response = post(replay_stub, unique("replay"), transfer_body(), replay_time=replay_time)
    assert response.status_code == 400, response.text
    assert [e["field"] for e in response.json()["errors"]] == ["X-Replay-Time"]
    assert fake.calls == 0


def test_a_bad_body_and_a_bad_replay_time_are_both_named(fake: FakeRisk, replay_stub: str) -> None:
    body = transfer_body() | {"amount": 0}
    response = post(replay_stub, unique("replay"), body, replay_time=None)
    assert response.status_code == 400
    assert [e["field"] for e in response.json()["errors"]] == ["amount", "X-Replay-Time"]
    assert fake.calls == 0


def test_a_replay_time_with_an_offset_is_read_as_utc_to_the_millisecond(
    fake: FakeRisk, replay_stub: str
) -> None:
    response = post(
        replay_stub,
        unique("replay"),
        transfer_body(),
        replay_time="2026-01-01T04:00:00.250999+01:00",
    )
    assert response.status_code == 201, response.text
    assert response.json()["createdAt"] == REPLAY_TIME


def test_the_same_key_and_body_replay_the_saved_201_without_asking(
    fake: FakeRisk, replay_stub: str
) -> None:
    body, key = transfer_body(), unique("replay")
    first = post(replay_stub, key, body)
    assert first.status_code == 201
    again = post(replay_stub, key, body)
    assert again.status_code == 201
    assert again.json() == first.json()
    assert fake.calls == 1


def test_normal_mode_ignores_the_replay_time(fake: FakeRisk, normal_stub: str) -> None:
    fake.respond_with(decided("hold"))
    body = transfer_body()
    held = post(normal_stub, unique("normal"), body)
    assert held.status_code == 403
    assert held.json()["type"] == "/problems/transfer-held"
    assert outbox_rows(body) == 0

    fake.respond_with(decided("allow"))
    allowed = post(normal_stub, unique("normal"), transfer_body())
    assert allowed.status_code == 201
    assert allowed.json()["createdAt"] != REPLAY_TIME
    assert fake.requests[-1]["requestedAt"] != REPLAY_TIME


# --- The replay stops at the first transfer that isn't posted at its time ---------------------


def transfers(n: int) -> list[Transfer]:
    return [
        Transfer(
            line=2 + i,
            step=1,
            type="TRANSFER",
            request_id=unique("replay"),
            sender=uuid.uuid4(),
            receiver=uuid.uuid4(),
            amount=100 + i,
            time_ms=STEP_1_MS + 1_000 * i,
            is_fraud=False,
        )
        for i in range(n)
    ]


class AppliedAtOnce:
    """A watermark behind the first transfer at the start, then past every one: these tests are
    about what the stub answers, not about the wait (test_replay_wait.py)."""

    def __init__(self) -> None:
        self.reads = 0

    def read(self) -> int:
        self.reads += 1
        return 0 if self.reads == 1 else 2**62


def test_a_transfer_without_a_decision_stops_the_replay(fake: FakeRisk, replay_stub: str) -> None:
    fake.respond_with(first_then(decided("allow"), raw(503)))
    with pytest.raises(ReplayError, match=r"transfer 1 \(line 3\) got 503"):
        replay(transfers(3), replay_stub, AppliedAtOnce())
    assert fake.calls == 2  # the third transfer is never asked about


def test_the_replay_stops_when_the_stub_isnt_in_replay_mode(
    fake: FakeRisk, normal_stub: str
) -> None:
    fake.respond_with(decided("allow"))
    with pytest.raises(ReplayError, match="STUB_REPLAY=1"):
        replay(transfers(2), normal_stub, AppliedAtOnce())
    assert fake.calls == 1
