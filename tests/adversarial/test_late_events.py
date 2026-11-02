"""Late events (design.md 3.2 step 3, D15, D40c). Phase 3, criterion 5: an event older than
watermark - 24 h - 5 min is dropped and counted; one just inside is applied.

First to the millisecond, with the consumer's loop and handler on database 15 of the stack's
Redis; then through the stack's Kafka and consumer, counted on its GET /metrics.
"""

from __future__ import annotations

import uuid

import redis
from confluent_kafka import Message

from risk import settings
from risk.features.consumer import FeatureConsumer, MessageHandler
from risk.features.redis_store import WATERMARK_KEY, LedgerWriter, ledger_key, member
from risk.settings import TOPIC
from tests.feature_state import STACK_DB, FakeSource, NoBadEvents, client, sample
from tests.helpers import CONSUMER_METRICS_URL, metric, now_ms, publish, wait_until_consumed
from tests.ledger_events import MINUTE_US, Event, transfer

DAY_MS = 24 * 3600 * 1000
CUTOFF_MS = DAY_MS + settings.lateness_margin_ms()  # D40c: 24 h + 5 min behind the watermark
LATE = "risk_consumer_events_total"


def kafka_message(event: Event, offset: int) -> Message:
    return Message(topic=TOPIC, partition=0, offset=offset, key=event.key(), value=event.value())


def has_member(db: redis.Redis, event: Event, account: uuid.UUID, amount: int) -> bool:
    return db.zscore(ledger_key(account, "out"), member(amount, str(event.id))) is not None


def test_to_the_millisecond_older_is_dropped_and_counted_and_at_the_cutoff_applied(
    scratch: redis.Redis,
) -> None:
    newest_ms = 1_793_000_000_000
    a, b, c, d = (uuid.uuid4() for _ in range(4))
    newest = transfer(a, b, 1, newest_ms * 1000)
    too_old = transfer(c, d, 5, (newest_ms - CUTOFF_MS - 1) * 1000)
    at_cutoff = transfer(c, d, 6, (newest_ms - CUTOFF_MS) * 1000)
    source = FakeSource([kafka_message(e, n) for n, e in enumerate([newest, too_old, at_cutoff])])
    handler = MessageHandler(LedgerWriter(scratch, settings.lateness_margin_ms()), NoBadEvents())
    late_before = sample(LATE, outcome="late")
    applied_before = sample(LATE, outcome="applied")

    scratch.flushdb()
    FeatureConsumer(source, handler).poll()

    assert sample(LATE, outcome="late") == late_before + 1
    assert sample(LATE, outcome="applied") == applied_before + 2
    assert not has_member(scratch, too_old, c, 5)
    assert has_member(scratch, at_cutoff, c, 6)
    # Dropped or applied, every offset is committed (D15).
    assert source.calls == [("commit", [(0, 3)])]
    # A late event never moves the watermark.
    assert int(scratch.get(WATERMARK_KEY) or 0) == newest_ms


def test_through_the_stack_a_late_event_is_dropped_and_counted() -> None:
    db = client(STACK_DB)
    a, b, c, d = (uuid.uuid4() for _ in range(4))
    # Not in the future, and not behind the watermark already applied.
    newest_ms = max(now_ms(), int(db.get(WATERMARK_KEY) or 0))
    newest = transfer(a, b, 1, newest_ms * 1000)
    wait_until_consumed(publish([(newest.key(), newest.value())]))

    too_old = transfer(c, d, 5, (newest_ms - CUTOFF_MS) * 1000 - 10 * MINUTE_US)
    just_inside = transfer(c, d, 6, (newest_ms - CUTOFF_MS) * 1000 + MINUTE_US)
    late_before = metric(CONSUMER_METRICS_URL, LATE, outcome="late")
    wait_until_consumed(publish([(e.key(), e.value()) for e in (too_old, just_inside)]))

    assert not has_member(db, too_old, c, 5)
    assert has_member(db, just_inside, c, 6)
    assert metric(CONSUMER_METRICS_URL, LATE, outcome="late") == late_before + 1
