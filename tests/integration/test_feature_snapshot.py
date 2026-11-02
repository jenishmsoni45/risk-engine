"""The 31-feature snapshot with its lag (design.md 3.4, I5). Phase 3, criterion 9: every decision
row holds all 31 features and the lag, the migration refuses a snapshot missing a name, and the
logged features equal the oracle's at that t.

Events go through the stack's Kafka and consumer, decisions through the stack's API; the oracle
recomputes from the events published and the decision rows of the sender.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest
import redis
from psycopg import errors
from psycopg.types.json import Jsonb

from risk.features.compute import FEATURE_NAMES
from risk.features.redis_store import WATERMARK_KEY, ledger_key
from risk.timestamps import epoch_ms, parse_offset_timestamp
from tests import oracle
from tests.feature_state import STACK_DB, client
from tests.helpers import (
    database,
    decision_request,
    decision_row,
    insert_decision,
    iso_ms,
    now_ms,
    post_decision,
    publish,
    wait_until_consumed,
)
from tests.ledger_events import HOUR_US, MINUTE_US, SECOND_US, Event, entry, transfer

ZERO = dict.fromkeys(FEATURE_NAMES, 0)


def watermark(db: redis.Redis) -> int:
    raw = db.get(WATERMARK_KEY)
    return 0 if raw is None else int(raw)


def logged(request_id: str) -> tuple[uuid.UUID, uuid.UUID, int, int, dict[str, int], int]:
    with database() as conn:
        row = conn.execute(
            "select from_account_id, to_account_id, amount, requested_at, features, "
            "feature_lag_ms from decisions where request_id = %s",
            (request_id,),
        ).fetchone()
    assert row is not None
    sender, receiver, amount, requested_at, features, lag = row
    return sender, receiver, amount, epoch_ms(requested_at), features, lag


def attempts_of(sender: uuid.UUID) -> list[oracle.OracleAttempt]:
    """The sender's decision requests, from the decision log (D18)."""
    with database() as conn:
        rows = conn.execute(
            "select request_id, requested_at, amount from decisions where from_account_id = %s",
            (sender,),
        ).fetchall()
    return [oracle.OracleAttempt(r, sender, epoch_ms(at), amount) for r, at, amount in rows]


def test_logged_features_equal_the_oracles_at_t() -> None:
    sender, receiver, x, y = (uuid.uuid4() for _ in range(4))
    t = now_ms()
    t_us = t * 1000
    events: list[Event] = [
        transfer(sender, x, 1500, t_us - 30 * SECOND_US, digits=6),
        transfer(sender, y, 2500, t_us - 10 * MINUTE_US, digits=0),
        transfer(x, sender, 7000, t_us - 3 * HOUR_US, digits=9),
        transfer(y, receiver, 900, t_us - 20 * SECOND_US),
        transfer(receiver, x, 400, t_us - 2 * HOUR_US),
        transfer(receiver, sender, 333, t_us - 59 * MINUTE_US),
        # -100 and +30 on the sender: one outflow of 70.
        entry([(sender, -100), (x, 70), (sender, 30)], t_us - 45 * SECOND_US),
        entry([(receiver, 500), (sender, -500)], t_us - 5 * MINUTE_US),
        # Nets to zero for the sender and the receiver: adds nothing for them.
        entry([(sender, 50), (sender, -50), (receiver, 1), (receiver, -1)], t_us - 40 * SECOND_US),
        entry([(x, 1500), (sender, -1500)], t_us - 50 * SECOND_US, kind="reversal"),
        # Outside every window: before the 24 h window, and at t itself.
        transfer(sender, x, 999, t_us - 24 * HOUR_US - 2 * SECOND_US),
        transfer(sender, receiver, 888, t_us),
    ]
    # One entry delivered twice, as an outbox relay may (I2).
    messages = [(e.key(), e.value()) for e in events] + [(events[0].key(), events[0].value())]
    wait_until_consumed(publish(messages))

    # Earlier requests by the sender, which the decision counts as attempts (D18).
    for shift_s in (300, 50, 2):
        early = decision_request(fromAccountId=str(sender), requestedAt=iso_ms(t - shift_s * 1000))
        assert post_decision(early).status_code == 200

    db = client(STACK_DB)
    before = watermark(db)
    body = decision_request(
        fromAccountId=str(sender), toAccountId=str(receiver), amount=4321, requestedAt=iso_ms(t)
    )
    response = post_decision(body)
    after = watermark(db)
    assert response.status_code == 200

    _, _, amount, logged_t, features, lag = logged(body["requestId"])
    assert logged_t == t
    assert set(features) == set(FEATURE_NAMES)
    expected = oracle.features(
        [e.oracle() for e in events], attempts_of(sender), sender, receiver, amount, t
    )
    assert features == expected
    # The oracle's answer is far from empty, so the comparison means something: the sender sent
    # 1500 and a net 70 in the last minute, then 2500 and 500 within the hour.
    assert (expected["out_count_1m"], expected["out_sum_1m"]) == (2, 1570)
    assert (expected["out_count_1h"], expected["out_sum_1h"]) == (4, 4570)
    assert (expected["attempt_count_1m"], expected["dest_in_count_1m"]) == (2, 1)
    # lag = t - wm, with the watermark the API read; it only ever rises (design.md 3.2).
    assert t - after <= lag <= t - before


def test_every_row_holds_all_31_features_and_the_lag() -> None:
    request_ids = []
    for amount in (1, 77, 2**63 - 1):
        body = decision_request(amount=amount)
        assert post_decision(body).status_code == 200
        request_ids.append(body["requestId"])
    for request_id in request_ids:
        _, _, amount, _, features, lag = logged(request_id)
        assert set(features) == set(FEATURE_NAMES)
        assert features == ZERO | {"amount": amount}
        assert isinstance(lag, int)


def test_a_feature_read_from_redis_matches_the_member_written() -> None:
    """A member written straight into the stack's Redis, as the consumer writes it, reaches the
    snapshot: the decision reads the sets it should and no others."""
    sender, receiver = uuid.uuid4(), uuid.uuid4()
    t = now_ms()
    db = client(STACK_DB)
    key = ledger_key(receiver, "out")
    db.zadd(key, {f"64:{uuid.uuid4()}": t - 1000})
    try:
        body = decision_request(
            fromAccountId=str(sender), toAccountId=str(receiver), requestedAt=iso_ms(t)
        )
        assert post_decision(body).status_code == 200
    finally:
        db.delete(key)
    features = logged(body["requestId"])[4]
    window = {"dest_out_count_1m": 1, "dest_out_sum_1m": 64}
    window |= {"dest_out_count_1h": 1, "dest_out_sum_1h": 64}
    window |= {"dest_out_count_24h": 1, "dest_out_sum_24h": 64}
    assert features == ZERO | window | {"amount": body["amount"]}


def snapshot_without(name: str) -> dict[str, int]:
    return {n: 0 for n in FEATURE_NAMES if n != name} | ({} if name == "amount" else {"amount": 1})


BAD_SNAPSHOTS: dict[str, Any] = {
    **{f"missing {name}": snapshot_without(name) for name in FEATURE_NAMES},
    "Phase 2's amount alone": {"amount": 1},
    "an extra name": ZERO | {"amount": 1, "score": 3},
    "a fraction": ZERO | {"amount": 1, "out_sum_1h": 1.5},
    "a string": ZERO | {"amount": "1"},
    "a null": ZERO | {"amount": 1, "in_count_1m": None},
    "an object": ZERO | {"amount": 1, "in_count_1m": {"value": 0}},
    "an array": ZERO | {"amount": 1, "in_count_1m": [0]},
}


@pytest.mark.parametrize("snapshot", BAD_SNAPSHOTS.values(), ids=BAD_SNAPSHOTS.keys())
def test_the_schema_refuses_a_snapshot_that_is_not_the_31_features(snapshot: Any) -> None:
    with pytest.raises(errors.CheckViolation, match="decisions_feature_names"):
        insert_decision(decision_row(features=Jsonb(snapshot)))


def test_the_schema_accepts_the_31_features_at_their_largest() -> None:
    insert_decision(decision_row(features=Jsonb(dict.fromkeys(FEATURE_NAMES, 2**63 - 1))))


def test_a_corrupt_member_answers_503_and_stores_nothing() -> None:
    sender = uuid.uuid4()
    db = client(STACK_DB)
    key = ledger_key(sender, "out")
    db.zadd(key, {"not-an-amount": now_ms() - 1000})
    try:
        body = decision_request(fromAccountId=str(sender), requestedAt=iso_ms(now_ms()))
        assert post_decision(body).status_code == 503
    finally:
        db.delete(key)
    with database() as conn:
        row = conn.execute(
            "select count(*) from decisions where request_id = %s", (body["requestId"],)
        ).fetchone()
    assert row == (0,)


def test_lag_is_t_minus_the_watermark_and_negative_when_the_stream_is_ahead() -> None:
    db = client(STACK_DB)
    wm = watermark(db)
    assert wm > 0, "an earlier test applied events"
    body = decision_request(requestedAt=iso_ms(wm - 60_000))
    assert post_decision(body).status_code == 200
    t = epoch_ms(parse_offset_timestamp(body["requestedAt"]))
    lag = logged(body["requestId"])[5]
    assert lag <= t - wm < 0
