"""I1: the same requestId always returns the same decision (design.md section 6).

Hypothesis generates sets of transfers, sends each one several times concurrently, in a shuffled
order and with different requestedAt values, to the running API. Every requestId must end with
one row and one answer. Phase 2, criteria 3 and 4.
"""

from __future__ import annotations

import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import httpx
from hypothesis import given, settings
from hypothesis import strategies as st

from tests.helpers import database, decision_request, decision_rows, post_decision

# Every character a requestId may hold (tally's Idempotency-Key pattern).
KEY_CHARACTERS = st.characters(min_codepoint=0x21, max_codepoint=0x7E)
ANSWER = ("decisionId", "decision", "ruleSet", "fired")
RETRIES = 20


@st.composite
def duplicated_requests(draw: st.DrawFn) -> list[tuple[int, dict[str, Any]]]:
    """(transfer number, request body) pairs: each transfer 1 to 6 times, shuffled."""
    suffixes = draw(
        st.lists(
            st.text(KEY_CHARACTERS, min_size=1, max_size=200), min_size=1, max_size=5, unique=True
        )
    )
    run = uuid.uuid4().hex  # keeps this example's requestIds new on a database that persists
    sends: list[tuple[int, dict[str, Any]]] = []
    for number, suffix in enumerate(suffixes):
        transfer = decision_request(
            requestId=f"{run}:{suffix}",
            amount=draw(st.integers(min_value=1, max_value=2**63 - 1)),
            currency=draw(st.sampled_from(["CAD", "USD", "EUR"])),
        )
        for second in draw(st.lists(st.integers(0, 59), min_size=1, max_size=6)):
            # requestedAt is not part of the match (D11), so the copies vary it.
            copy = {**transfer, "requestedAt": f"2026-11-20T15:04:{second:02d}.123Z"}
            sends.append((number, copy))
    return draw(st.permutations(sends))


def send_until_answered(body: dict[str, Any], start: threading.Barrier) -> httpx.Response:
    """Sends the request; a 503 (no decision) is asked again, as a caller would."""
    start.wait()
    for _ in range(RETRIES):
        response = post_decision(body)
        if response.status_code != 503:
            return response
        time.sleep(0.05)
    raise AssertionError(f"still 503 after {RETRIES} tries")


@given(duplicated_requests())
@settings(max_examples=25)
def test_concurrent_duplicates_get_one_row_and_one_answer(
    sends: list[tuple[int, dict[str, Any]]],
) -> None:
    start = threading.Barrier(len(sends))
    with ThreadPoolExecutor(max_workers=len(sends)) as pool:
        futures = [(n, body, pool.submit(send_until_answered, body, start)) for n, body in sends]
        results = [(n, body, future.result()) for n, body, future in futures]

    answers: dict[int, set[tuple[Any, ...]]] = {}
    for number, _, response in results:
        assert response.status_code == 200, response.text
        body = response.json()
        answers.setdefault(number, set()).add(
            tuple(tuple(body[k]) if k == "fired" else body[k] for k in ANSWER)
        )
    for number, answer_set in answers.items():
        assert len(answer_set) == 1, f"transfer {number} got different answers: {answer_set}"

    for number, body, _ in results:
        rows = decision_rows(body["requestId"])
        assert len(rows) == 1
        (answer,) = answers[number]
        assert str(rows[0][0]) == answer[0]


def full_row(request_id: str) -> tuple[Any, ...]:
    with database() as conn:
        row = conn.execute(
            "select * from decisions where request_id = %s", (request_id,)
        ).fetchone()
    assert row is not None
    return row


def test_conflict() -> None:
    """Criterion 4: a changed transfer gets 409 and changes nothing; requestedAt alone replays."""
    body = decision_request()
    first = post_decision(body)
    assert first.status_code == 200
    stored = full_row(body["requestId"])

    for change in (
        {"fromAccountId": str(uuid.uuid4())},
        {"toAccountId": str(uuid.uuid4())},
        {"amount": body["amount"] + 1},
        {"currency": "USD"},
    ):
        response = post_decision({**body, **change})
        assert response.status_code == 409, change
        assert response.headers["content-type"] == "application/problem+json"
        assert response.json()["status"] == 409
    assert full_row(body["requestId"]) == stored

    replay = post_decision({**body, "requestedAt": "2027-01-01T00:00:00+05:00"})
    assert replay.status_code == 200
    assert replay.json() == first.json()
    assert full_row(body["requestId"]) == stored


def test_concurrent_conflicting_transfers_get_one_row() -> None:
    """Two different transfers racing for one requestId: one wins, the other gets 409."""
    request_id = f"race-{uuid.uuid4().hex}"
    bodies = [decision_request(requestId=request_id, amount=amount) for amount in range(1, 9)]
    start = threading.Barrier(len(bodies))
    with ThreadPoolExecutor(max_workers=len(bodies)) as pool:
        responses = list(pool.map(lambda b: send_until_answered(b, start), bodies))
    statuses = sorted(r.status_code for r in responses)
    assert statuses == [200] + [409] * (len(bodies) - 1)
    assert len(decision_rows(request_id)) == 1
