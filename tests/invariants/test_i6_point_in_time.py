"""I6 (design.md section 6): features use only events before the decision time. Phase 6,
criterion 5: events at or after t change no feature, online and offline.

Online: a log of events and attempts before t goes through the consumer's handler and the
decision API's attempt write into Redis (database 15, as in I2 and I3), and the 31 features at t
are read the API's way, for every pair of accounts. Then Hypothesis adds events and attempts at
or after t, t itself most of all, and the features at t must not change. The added times stay
within the lateness margin after t, inside the retained range where I3 holds (D15, D40c): an event
a day later would move the watermark and trim what t needs, which says nothing about t's past.

Offline: a transfer at t is given its features by the offline pipeline (ml/offline.py), alone
after its past, then with transfers at t, before or after it in the input, and later ones added;
its features must not change. Both start from features equal to the oracle's, so the check is
not between two wrong answers.
"""

from __future__ import annotations

import uuid

import redis
from hypothesis import given, settings
from hypothesis import strategies as st

from ml.offline import features
from ml.paysim import Transfer
from tests import oracle
from tests.feature_state import SCRATCH_DB, Query, deliver, read, record_attempts
from tests.ledger_events import (
    ACCOUNTS,
    DAY_US,
    HOUR_US,
    MINUTE_US,
    Event,
    deliveries,
    event_logs,
    transfer,
)

T_US = (1_793_000_000 + 30 * 3600) * 1_000_000  # a whole second, so 0 fraction digits stay at t
T_MS = T_US // 1000
MARGIN_US = 5 * MINUTE_US  # D40c, the default the services run with
DAY_MS, HOUR_MS, MINUTE_MS = DAY_US // 1000, HOUR_US // 1000, MINUTE_US // 1000

# Before t: anywhere in the 26 hours before it, and at every window's edge.
PAST_US = st.one_of(
    st.integers(T_US - DAY_US - 2 * HOUR_US, T_US - 1),
    st.sampled_from(
        [T_US - w * 1000 + shift for w in (MINUTE_MS, HOUR_MS, DAY_MS) for shift in (-1000, 0)]
        + [T_US - 1000]
    ),
)
# At or after t: t itself most of all, then anywhere in the margin after it.
FUTURE_US = st.one_of(st.just(T_US), st.integers(T_US, T_US + MARGIN_US))


@st.composite
def attempts(
    draw: st.DrawFn, times_us: st.SearchStrategy[int], prefix: str
) -> list[oracle.OracleAttempt]:
    return [
        oracle.OracleAttempt(
            request_id=f"{prefix}:{n}",
            sender=draw(st.sampled_from(ACCOUNTS)),
            time_ms=draw(times_us) // 1000,
            amount=draw(st.integers(1, 10**6)),
        )
        for n in range(draw(st.integers(0, 8)))
    ]


QUERIES = [Query(s, r, 7, T_MS) for s in ACCOUNTS for r in ACCOUNTS if s != r]


@settings(max_examples=100)
@given(
    past=event_logs(PAST_US, max_size=20),
    future=event_logs(FUTURE_US, max_size=10),
    past_attempts=attempts(PAST_US, "past"),
    future_attempts=attempts(FUTURE_US, "future"),
    data=st.data(),
)
def test_online_events_at_or_after_t_change_no_feature(
    scratch: redis.Redis,
    past: list[Event],
    future: list[Event],
    past_attempts: list[oracle.OracleAttempt],
    future_attempts: list[oracle.OracleAttempt],
    data: st.DataObject,
) -> None:
    future = [e for e in future if e.id not in {p.id for p in past}]
    scratch.flushdb()
    deliver(scratch, past, data.draw(deliveries(len(past)), label="past order"))
    record_attempts(SCRATCH_DB, past_attempts)
    before = [f for f, _ in read(SCRATCH_DB, QUERIES)]
    entries = [e.oracle() for e in past]
    for q, got in zip(QUERIES, before, strict=True):
        assert got == oracle.features(entries, past_attempts, q.sender, q.receiver, 7, T_MS), q

    if future:
        deliver(scratch, future, data.draw(deliveries(len(future)), label="future order"))
    record_attempts(SCRATCH_DB, future_attempts)
    after = [f for f, _ in read(SCRATCH_DB, QUERIES)]
    assert after == before


@st.composite
def transfers(draw: st.DrawFn, times_ms: st.SearchStrategy[int]) -> Transfer:
    sender, receiver = draw(
        st.lists(st.sampled_from(ACCOUNTS), min_size=2, max_size=2, unique=True)
    )
    return Transfer(
        line=0,
        step=1,
        type=draw(st.sampled_from(("TRANSFER", "CASH_OUT"))),
        request_id=str(draw(st.uuids())),
        sender=sender,
        receiver=receiver,
        amount=draw(st.integers(1, 10**9)),
        time_ms=draw(times_ms),
        is_fraud=False,
    )


def transfer_logs(
    times_ms: st.SearchStrategy[int], max_size: int
) -> st.SearchStrategy[list[Transfer]]:
    return st.lists(transfers(times_ms), max_size=max_size, unique_by=lambda t: t.request_id)


PAST_MS = PAST_US.map(lambda us: us // 1000)
# Offline keeps no retention range, so the future reaches well past a day after t.
FUTURE_MS = st.one_of(st.just(T_MS), st.integers(T_MS, T_MS + 2 * DAY_MS))


def by_time(log: list[Transfer]) -> list[Transfer]:
    return sorted(log, key=lambda t: t.time_ms)


@settings(max_examples=150)
@given(
    past=transfer_logs(PAST_MS, 20),
    target=transfers(st.just(T_MS)),
    future=transfer_logs(FUTURE_MS, 12),
    target_first=st.booleans(),
)
def test_offline_transfers_at_or_after_t_change_no_feature(
    past: list[Transfer], target: Transfer, future: list[Transfer], target_first: bool
) -> None:
    taken = {t.request_id for t in past} | {target.request_id}
    future = [t for t in future if t.request_id not in taken]
    alone_log = [*by_time(past), target]
    alone = list(features(alone_log))[-1].features
    entries, attempts_ = oracle.replayed(alone_log)
    assert alone == oracle.features(
        entries, attempts_, target.sender, target.receiver, target.amount, T_MS
    )

    at_t = [t for t in future if t.time_ms == T_MS]
    later = by_time([t for t in future if t.time_ms > T_MS])
    group = [target, *at_t] if target_first else [*at_t, target]
    combined = [*by_time(past), *group, *later]
    (with_future,) = [r.features for r in features(combined) if r.transfer is target]
    assert with_future == alone


def test_an_event_and_an_attempt_exactly_at_t_count_for_nothing(scratch: redis.Redis) -> None:
    """The case I6 turns on, spelled out: t's own millisecond is not before t (D40a)."""
    a, b = ACCOUNTS[0], ACCOUNTS[1]
    scratch.flushdb()
    deliver(scratch, [transfer(a, b, 5, T_US - 1000)], [0])
    record_attempts(SCRATCH_DB, [oracle.OracleAttempt("before", a, T_MS - 1, 5)])
    query = [Query(a, b, 7, T_MS)]
    ((before, _),) = read(SCRATCH_DB, query)
    assert (before["out_count_1m"], before["attempt_count_1m"], before["dest_in_count_1m"]) == (
        1,
        1,
        1,
    )

    deliver(scratch, [transfer(a, b, 9, T_US), transfer(b, a, 9, T_US)], [0, 1])
    record_attempts(SCRATCH_DB, [oracle.OracleAttempt("at-t", a, T_MS, 9)])
    ((after, _),) = read(SCRATCH_DB, query)
    assert after == before

    log = [
        Transfer(0, 1, "TRANSFER", str(uuid.UUID(int=n)), s, r, amount, time_ms, False)
        for n, (s, r, amount, time_ms) in enumerate(
            [(a, b, 5, T_MS - 1), (a, b, 7, T_MS), (a, b, 9, T_MS), (b, a, 9, T_MS)]
        )
    ]
    # Offline, the same past gives the transfers at t the same features, whatever else is at t.
    offline = [r.features for r in features(log)]
    assert offline[1] == before
    assert offline[2] == before | {"amount": 9}
