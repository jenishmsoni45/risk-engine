"""Redis data lost (design.md section 7, 3.5; D40r, D48.1). Phase 5, criterion 4: after Redis is
flushed, `make rebuild-features` restores features equal to the oracle's.

Ledger events go through the stack's Kafka and consumer, and attempts through decisions made by
the stack's API, so the sources the rebuild reads are the real ones. Then the stack's Redis is
flushed, watermark included, so the rebuild replays from the start of the topic; the features
read afterwards, the way the decision API reads them, equal the oracle's recompute.
"""

from __future__ import annotations

import subprocess
import uuid

from risk.features.redis_store import WATERMARK_KEY
from tests import oracle
from tests.feature_state import STACK_DB, Query, client, read
from tests.helpers import (
    ROOT,
    decision_request,
    iso_ms,
    now_ms,
    post_decision,
    publish,
    wait_until_consumed,
)
from tests.ledger_events import HOUR_US, MINUTE_US, SECOND_US, Event, entry, transfer
from tests.oracle import OracleAttempt


def test_after_redis_is_flushed_rebuild_features_restores_features_equal_to_the_oracles() -> None:
    db = client(STACK_DB)
    # Not behind the watermark already applied, so none of these events is late.
    base = max(now_ms(), int(db.get(WATERMARK_KEY) or 0))
    base_us = base * 1000
    sender, receiver, x, y = (uuid.uuid4() for _ in range(4))

    events: list[Event] = [
        transfer(sender, x, 100 + n, base_us - (n + 1) * 7 * SECOND_US) for n in range(10)
    ]
    events += [transfer(x, receiver, 300 + n, base_us - (n + 1) * 9 * MINUTE_US) for n in range(8)]
    events += [entry([(sender, -1000), (receiver, 600), (y, 400)], base_us - 2 * HOUR_US)]
    events += [entry([(receiver, -50), (sender, 50)], base_us - 20 * HOUR_US)]
    events += [entry([(sender, -70), (x, 70)], base_us - 3 * MINUTE_US, kind="reversal")]
    wait_until_consumed(publish([(e.key(), e.value()) for e in events]))

    attempts = []
    for offset_ms, amount in ((30_000, 11), (2 * 3600_000, 12), (23 * 3600_000, 13)):
        body = decision_request(
            fromAccountId=str(sender),
            toAccountId=str(receiver),
            amount=amount,
            requestedAt=iso_ms(base - offset_ms),
        )
        assert post_decision(body).status_code == 200
        attempts.append(OracleAttempt(body["requestId"], sender, base - offset_ms, amount))

    # Inside the retained range: no earlier than 5 min before the watermark.
    queries = [Query(sender, receiver, 1, t) for t in (base + 1, base - 60_000, base - 240_000)]
    expected = [
        oracle.features([e.oracle() for e in events], attempts, q.sender, q.receiver, 1, q.t_ms)
        for q in queries
    ]
    assert [features for features, _ in read(STACK_DB, queries)] == expected
    assert expected[0]["out_count_1m"] > 0
    assert expected[0]["attempt_count_24h"] == 3

    db.flushdb()
    emptied = read(STACK_DB, queries)
    assert all(watermark == 0 for _, watermark in emptied)
    assert emptied[0][0]["out_count_1m"] == 0

    rebuilt = subprocess.run(
        ["make", "rebuild-features"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    assert rebuilt.returncode == 0, rebuilt.stdout + rebuilt.stderr
    # No watermark was left, so the replay started at the beginning of the topic (D48.1).
    assert "watermark_before: None" in rebuilt.stdout

    restored = read(STACK_DB, queries)
    assert [features for features, _ in restored] == expected
    assert all(watermark >= max(e.time_ms for e in events) for _, watermark in restored)
