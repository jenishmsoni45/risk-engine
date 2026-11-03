"""CONSUMER_GROUP and CONSUMER_START (D48.6), which make e2e uses: a new group told to start at
the latest event commits the topic's end before it reads anything, skips what was already there,
and applies what comes after.

The real consumer loop runs on this machine in a new group, against the stack's Kafka, writing to
database 15 of the stack's Redis, which no service uses.
"""

from __future__ import annotations

import threading
import uuid

import redis

from risk import settings
from risk.features.consumer import ConsumerConfig, run
from risk.features.redis_store import ledger_key, member
from tests.feature_state import SCRATCH_DB, redis_url
from tests.helpers import (
    KAFKA_HOST_BOOTSTRAP,
    ConsumerOffsets,
    free_port,
    now_ms,
    postgres_url,
    publish,
    topic_ends,
    unique,
)
from tests.ledger_events import Event, transfer


def applied(db: redis.Redis, event: Event, sender: uuid.UUID, amount: int) -> bool:
    return db.zscore(ledger_key(sender, "out"), member(amount, str(event.id))) is not None


def test_a_new_group_started_at_the_latest_event_skips_the_past_and_reads_what_follows(
    scratch: redis.Redis,
) -> None:
    sender, receiver = uuid.uuid4(), uuid.uuid4()
    t_us = now_ms() * 1000
    before = transfer(sender, receiver, 11, t_us)
    after = transfer(sender, receiver, 22, t_us + 1000)
    publish([(before.key(), before.value())])
    ends = topic_ends()

    group = unique("risk-features-start-test")
    config = ConsumerConfig(
        bootstrap=KAFKA_HOST_BOOTSTRAP,
        redis_url=redis_url(SCRATCH_DB),
        database_url=postgres_url(),
        margin_ms=settings.lateness_margin_ms(),
        metrics_port=free_port(),
        group=group,
        start="latest",
    )
    stop = threading.Event()
    thread = threading.Thread(target=run, args=(config, stop), daemon=True)
    offsets = ConsumerOffsets(group)
    thread.start()
    try:
        # The end of every partition is committed before anything is read (start_at_end).
        offsets.wait_past([(p, end - 1) for p, end in ends.items()], timeout=60)
        assert offsets.committed() == ends

        offsets.wait_past(publish([(after.key(), after.value())]), timeout=30)
        assert applied(scratch, after, sender, 22)
        assert not applied(scratch, before, sender, 11)
    finally:
        stop.set()
        thread.join(timeout=30)
        offsets.close()
    assert not thread.is_alive()
