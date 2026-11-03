"""Kafka's log outlives its container (D47, D48.4): the log is in a named volume, as Postgres's
data is, so a recreated broker keeps ledger.entries and its offsets.

Before the fix the log lived inside the container: `make down` then `make up` started the topic
again at offset 0 while bad_events kept its rows, so a new bad event at an offset already used
was taken as stored and skipped, and the rebuild (design.md 3.5) found nothing to replay. The
test recreates only the broker's container, as `make down` and `make up` would, and checks that
an event published before is still at its offset, that new events get later offsets, and that a
bad event published after is stored under its own offset.
"""

from __future__ import annotations

import uuid

from confluent_kafka import Consumer, TopicPartition

from risk.settings import TOPIC
from tests.helpers import (
    KAFKA_HOST_BOOTSTRAP,
    compose,
    database,
    now_ms,
    publish,
    unique,
    wait_until_consumed,
    wait_until_healthy,
)
from tests.ledger_events import transfer


def read_at(partition: int, offset: int) -> bytes | None:
    consumer = Consumer({"bootstrap.servers": KAFKA_HOST_BOOTSTRAP, "group.id": unique("read-at")})
    try:
        consumer.assign([TopicPartition(TOPIC, partition, offset)])
        for _ in range(60):
            message = consumer.poll(0.5)
            if message is not None and message.error() is None:
                return message.key()
        return None
    finally:
        consumer.close()


def test_a_recreated_broker_keeps_the_log_and_its_offsets() -> None:
    event = transfer(uuid.uuid4(), uuid.uuid4(), 100, now_ms() * 1000)
    ((partition, offset),) = publish([(event.key(), event.value())], partition=0)
    wait_until_consumed([(partition, offset)])

    # What `make down` and `make up` do to the broker: a new container from the same config.
    compose("rm", "--stop", "--force", "kafka")
    try:
        compose("up", "--detach", "--wait", "--wait-timeout", "180", "kafka", timeout=240)
    finally:
        # The consumer and the stub reconnect on their own; wait until both are back in shape.
        for service in ("kafka", "consumer", "stub"):
            wait_until_healthy(service, timeout=180)

    assert read_at(partition, offset) == event.key()

    bad_value = f'{{"not": "an entry", "marker": "{uuid.uuid4()}"}}'.encode()
    ((bad_partition, bad_offset),) = publish([(b"bad", bad_value)], partition=0)
    assert bad_offset > offset
    wait_until_consumed([(bad_partition, bad_offset)], timeout=60)
    with database() as conn:
        stored = conn.execute(
            "select kafka_offset from bad_events where topic = %s and message_value = %s",
            (TOPIC, bad_value),
        ).fetchall()
    assert stored == [(bad_offset,)]
