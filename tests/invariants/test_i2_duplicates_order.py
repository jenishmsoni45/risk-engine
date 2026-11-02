"""I2: duplicate or reordered events never change feature values (Phase 3, criterion 1).

Hypothesis generates entries over four accounts within 23 hours, so none is ever late or trimmed,
and delivers them through the consumer's handler with repeats and in shuffled orders. The
features read the decision API's way then equal the oracle's over the distinct entries, for every
pair of accounts and at any query time, inside the entries' span or past it.
"""

from __future__ import annotations

import redis
from hypothesis import given, settings
from hypothesis import strategies as st

from tests import oracle
from tests.feature_state import SCRATCH_DB, Query, deliver, read
from tests.ledger_events import ACCOUNTS, HOUR_US, MINUTE_US, Event, deliveries, event_logs

START_US = 1_793_000_000 * 1_000_000  # 2026-10-27, an arbitrary fixed start
SPAN_US = 23 * HOUR_US
WIDTHS_MS = tuple(oracle.WIDTHS_MS.values())


def query_times(log: list[Event]) -> st.SearchStrategy[int]:
    """Any time from before the first entry to past every window, plus each window's edges."""
    low = (START_US - 2 * MINUTE_US) // 1000
    high = (START_US + SPAN_US + 25 * HOUR_US) // 1000
    edges = sorted(
        {e.time_ms + shift for e in log for w in (0, *WIDTHS_MS) for shift in (w, w + 1)}
    )
    return st.one_of(st.integers(low, high), st.sampled_from(edges))


@settings(max_examples=150)
@given(log=event_logs(st.integers(START_US, START_US + SPAN_US)), data=st.data())
def test_duplicates_and_order_never_change_features(
    scratch: redis.Redis, log: list[Event], data: st.DataObject
) -> None:
    order = data.draw(deliveries(len(log)), label="delivery order")
    scratch.flushdb()
    outcomes = deliver(scratch, log, order)
    assert "late" not in outcomes

    times = data.draw(st.lists(query_times(log), min_size=1, max_size=4), label="query times")
    amount = data.draw(st.integers(1, 10**6), label="amount")
    queries = [
        Query(sender, receiver, amount, t)
        for t in times
        for sender in ACCOUNTS
        for receiver in ACCOUNTS
        if sender != receiver
    ]
    entries = [e.oracle() for e in log]
    for q, (features, _) in zip(queries, read(SCRATCH_DB, queries), strict=True):
        assert features == oracle.features(entries, [], q.sender, q.receiver, q.amount, q.t_ms), q
