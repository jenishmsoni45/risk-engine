"""Attempts (design.md 3.3, D18, D40g). Phase 3, criterion 8: every new decision adds one attempt;
a replayed requestId adds none; attempt features equal a recompute from decisions.

The attempt set is read straight from the stack's Redis; the recompute uses the decision log and
the oracle, which shares no code with the feature module.
"""

from __future__ import annotations

import random
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from risk.features.redis_store import attempt_key
from risk.timestamps import epoch_ms, parse_offset_timestamp
from tests import oracle
from tests.feature_state import STACK_DB, client
from tests.helpers import database, decision_request, iso_ms, now_ms, post_decision


def attempt_set(sender: uuid.UUID) -> set[tuple[bytes, float]]:
    # redis-py types the reply loosely: with scores it is a list of (member, score) pairs.
    pairs: Any = client(STACK_DB).zrange(attempt_key(sender), 0, -1, withscores=True)
    return {(bytes(m), float(score)) for m, score in pairs}


def expected_set(bodies: list[dict[str, Any]]) -> set[tuple[bytes, float]]:
    """{amount}:{requestId} at requestedAt, one per request (architecture.md section 4)."""
    return {
        (
            f"{b['amount']}:{b['requestId']}".encode(),
            float(epoch_ms(parse_offset_timestamp(b["requestedAt"]))),
        )
        for b in bodies
    }


def test_every_new_decision_adds_one_attempt_and_a_replay_none() -> None:
    sender = uuid.uuid4()
    start = now_ms() - 10 * 60 * 1000
    bodies = [
        decision_request(
            fromAccountId=str(sender),
            # A colon in the requestId, which the member format must survive.
            requestId=f"attempt:{n}:{uuid.uuid4().hex}",
            amount=100 + n,
            requestedAt=iso_ms(start + n * 7000),
        )
        for n in range(6)
    ]
    for body in bodies:
        assert post_decision(body).status_code == 200
    assert attempt_set(sender) == expected_set(bodies)

    # Replays, a changed requestedAt included, and a 409 add nothing.
    for body in bodies[:3]:
        assert post_decision({**body, "requestedAt": iso_ms(start + 99_000)}).status_code == 200
    assert post_decision({**bodies[0], "amount": 1}).status_code == 409
    assert attempt_set(sender) == expected_set(bodies)


def test_refused_attempts_count() -> None:
    """Phase 4, criterion 5: held and blocked decisions add attempts too (D18)."""
    sender = uuid.uuid4()
    t = now_ms()
    bodies = [
        # Held by large_transfer, with no earlier attempt.
        decision_request(
            fromAccountId=str(sender), amount=1_000_000, requestedAt=iso_ms(t - 50_000)
        ),
        # Allowed: one to nine earlier attempts, under attempt_burst_1m's 10.
        *(
            decision_request(
                fromAccountId=str(sender), amount=100 + n, requestedAt=iso_ms(t - 45_000 + n * 4000)
            )
            for n in range(9)
        ),
        # Blocked by attempt_burst_1m: the held one is the 10th attempt the first of these sees,
        # and the first is the 11th the second sees.
        *(
            decision_request(fromAccountId=str(sender), amount=100, requestedAt=iso_ms(t + n))
            for n in range(2)
        ),
    ]
    answers = []
    for body in bodies:
        response = post_decision(body)
        assert response.status_code == 200
        answers.append((response.json()["decision"], response.json()["fired"]))
    assert answers == [
        ("hold", ["large_transfer"]),
        *[("allow", [])] * 9,
        *[("block", ["attempt_burst_1m"])] * 2,
    ]
    assert attempt_set(sender) == expected_set(bodies)

    with database() as conn:
        row = conn.execute(
            "select features from decisions where request_id = %s", (bodies[-1]["requestId"],)
        ).fetchone()
    assert row is not None
    assert row[0]["attempt_count_1m"] == 11


def test_concurrent_duplicates_of_a_new_request_add_one_attempt() -> None:
    sender = uuid.uuid4()
    body = decision_request(fromAccountId=str(sender), requestedAt=iso_ms(now_ms()))
    with ThreadPoolExecutor(max_workers=8) as pool:
        statuses = list(pool.map(lambda _: post_decision(body).status_code, range(8)))
    assert statuses == [200] * 8
    assert attempt_set(sender) == expected_set([body])


def test_attempt_features_equal_a_recompute_from_decisions() -> None:
    sender = uuid.uuid4()
    t = now_ms()
    # Spread over every window and its edges, and decided in a shuffled order, so later requests
    # are decided before earlier ones.
    shifts_ms = [23 * 3600_000, 90 * 60_000, 3600_000, 59 * 60_000, 61_000, 60_000, 59_000, 1_000]
    times = [t - shift for shift in shifts_ms] + [t - 30_000, t - 30_000]
    random.Random(7).shuffle(times)  # noqa: S311 - a fixed test order, not a secret
    request_ids = []
    for n, at in enumerate(times):
        body = decision_request(fromAccountId=str(sender), amount=10 + n, requestedAt=iso_ms(at))
        assert post_decision(body).status_code == 200
        request_ids.append(body["requestId"])

    with database() as conn:
        rows = conn.execute(
            "select request_id, to_account_id, amount, requested_at, features from decisions "
            "where from_account_id = %s",
            (sender,),
        ).fetchall()
    assert sorted(r[0] for r in rows) == sorted(request_ids)
    log = [oracle.OracleAttempt(r[0], sender, epoch_ms(r[3]), r[2]) for r in rows]

    # Each decision, made in turn, saw exactly the earlier-decided requests before its t. A request
    # decided later can't be in an earlier snapshot, so the recompute uses the decided-before ones.
    order = {request_id: n for n, request_id in enumerate(request_ids)}
    for request_id, receiver, amount, requested_at, features in rows:
        seen = [a for a in log if order[a.request_id] < order[request_id]]
        expected = oracle.features([], seen, sender, receiver, amount, epoch_ms(requested_at))
        assert features == expected, request_id

    # A decision at t, after all of them, counts every one in its windows.
    last = decision_request(fromAccountId=str(sender), amount=5, requestedAt=iso_ms(t))
    assert post_decision(last).status_code == 200
    with database() as conn:
        row = conn.execute(
            "select to_account_id, features from decisions where request_id = %s",
            (last["requestId"],),
        ).fetchone()
    assert row is not None
    assert row[1] == oracle.features([], log, sender, row[0], 5, t)
    assert (row[1]["attempt_count_24h"], row[1]["attempt_count_1m"]) == (len(times), 5)
