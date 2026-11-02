"""I3: window values equal a recompute from the raw event log (Phase 3, criterion 2).

Entries are spread over 60 hours and many sit near the lateness cutoff, and they arrive in
shuffled orders, so many come after the watermark has passed them: those older than
watermark - 24 h - 5 min are dropped, the rest applied, whenever they arrive (D15). Attempts are
recorded in shuffled orders too. One transfer sits at the newest time, so the final watermark is
known. At random query times inside the retained range (from the newest time - 5 min onwards,
where every window lies inside what is kept, D40c), every window read the decision API's way
equals the oracle's recompute over everything delivered, dropped events included.
"""

from __future__ import annotations

import redis
from hypothesis import given, settings
from hypothesis import strategies as st

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

START_US = 1_793_000_000 * 1_000_000  # 2026-10-27, an arbitrary fixed start
NEWEST_US = START_US + 60 * HOUR_US
NEWEST_MS = NEWEST_US // 1000
MARGIN_MS = 5 * 60 * 1000  # D40c, the default the services run with
WIDTHS_MS = tuple(oracle.WIDTHS_MS.values())

# Most times anywhere in the 60 hours; many within minutes of newest - 24 h - 5 min, the cutoff
# once the newest transfer has been applied.
CUTOFF_US = NEWEST_US - DAY_US - 5 * MINUTE_US
TIMES_US = st.one_of(
    st.integers(START_US, NEWEST_US),
    st.integers(CUTOFF_US - 3 * MINUTE_US, CUTOFF_US + 3 * MINUTE_US),
)


@st.composite
def attempt_logs(draw: st.DrawFn) -> list[oracle.OracleAttempt]:
    count = draw(st.integers(0, 12))
    return [
        oracle.OracleAttempt(
            request_id=f"req:{n}",  # with a colon, which a requestId may hold
            sender=draw(st.sampled_from(ACCOUNTS)),
            time_ms=draw(TIMES_US) // 1000,
            amount=draw(st.integers(1, 10**6)),
        )
        for n in range(count)
    ]


def retained_query_times(log: list[Event]) -> st.SearchStrategy[int]:
    low = NEWEST_MS - MARGIN_MS
    edges = sorted(
        t
        for t in {e.time_ms + shift for e in log for w in WIDTHS_MS for shift in (w, w + 1)}
        if t >= low
    )
    anywhere = st.integers(low, NEWEST_MS + 25 * 60 * 60 * 1000)
    return st.one_of(anywhere, st.sampled_from(edges)) if edges else anywhere


@settings(max_examples=150)
@given(log=event_logs(TIMES_US, max_size=25), attempts=attempt_logs(), data=st.data())
def test_windows_equal_a_recompute_from_the_log(
    scratch: redis.Redis,
    log: list[Event],
    attempts: list[oracle.OracleAttempt],
    data: st.DataObject,
) -> None:
    log = [*log, transfer(ACCOUNTS[0], ACCOUNTS[1], 1, NEWEST_US)]
    order = data.draw(deliveries(len(log)), label="delivery order")
    attempt_order = data.draw(deliveries(len(attempts)), label="attempt order") if attempts else []
    scratch.flushdb()
    deliver(scratch, log, order)
    record_attempts(SCRATCH_DB, [attempts[i] for i in attempt_order])

    times = data.draw(st.lists(retained_query_times(log), min_size=1, max_size=4), label="t")
    queries = [
        Query(sender, receiver, 7, t)
        for t in times
        for sender in ACCOUNTS
        for receiver in ACCOUNTS
        if sender != receiver
    ]
    entries = [e.oracle() for e in log]
    for q, (features, watermark) in zip(queries, read(SCRATCH_DB, queries), strict=True):
        assert watermark == NEWEST_MS
        expected = oracle.features(entries, attempts, q.sender, q.receiver, q.amount, q.t_ms)
        assert features == expected, q


def test_the_log_does_hold_late_events(scratch: redis.Redis) -> None:
    """The cutoff the test aims at is the one the consumer applies: delivered after the newest
    transfer, an event 1 ms older than the cutoff is dropped and one at the cutoff is applied."""
    newest = transfer(ACCOUNTS[0], ACCOUNTS[1], 1, NEWEST_US)
    dropped = transfer(ACCOUNTS[2], ACCOUNTS[3], 5, CUTOFF_US - 1000)
    kept = transfer(ACCOUNTS[2], ACCOUNTS[3], 6, CUTOFF_US)
    scratch.flushdb()
    assert deliver(scratch, [newest, dropped, kept], [0, 1, 2]) == ["applied", "late", "applied"]
