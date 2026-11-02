"""The consumer's retry against real Kafka (design.md section 7): a message that fails because a
dependency did is read again, its partition waits meanwhile, and the other partitions keep
flowing. tests/unit/test_consumer_loop.py checks the calls with a fake; this checks that
librdkafka's seek, pause and resume really give that, with nothing skipped or reordered.

The loop runs in this process with a real confluent_kafka.Consumer in a consumer group of its
own, starting at the end each partition had, so the stack's consumer is untouched. The handler
only records what it is given; it fails one chosen message the first time.
"""

from __future__ import annotations

import time
import uuid

from confluent_kafka import Consumer, Message, TopicPartition

from risk.features.consumer import FeatureConsumer, Outcome, RetryLaterError, kafka_settings
from risk.settings import TOPIC
from tests.helpers import KAFKA_HOST_BOOTSTRAP, publish

PER_PARTITION = 8


class FailsOnce:
    def __init__(self, failing_key: bytes) -> None:
        self.failing_key = failing_key
        self.failed = False
        self.handled: list[tuple[int, int, bytes]] = []  # successes, in order

    def handle(self, message: Message) -> Outcome:
        key = message.key() or b""
        if key == self.failing_key and not self.failed:
            self.failed = True
            raise RetryLaterError("postgres")
        self.handled.append((message.partition() or 0, message.offset() or 0, key))
        return "applied"


def test_a_failed_message_is_read_again_and_no_partition_skips_or_reorders() -> None:
    run = uuid.uuid4().hex
    keys = {p: [f"{run}-{p}-{n}".encode() for n in range(PER_PARTITION)] for p in range(3)}
    handler = FailsOnce(failing_key=keys[0][2])
    consumer = Consumer({**kafka_settings(KAFKA_HOST_BOOTSTRAP), "group.id": f"test-retry-{run}"})
    # Start each partition at its current end, so only this test's messages are read.
    ends = {
        p: consumer.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=10)[1] for p in range(3)
    }
    loop = FeatureConsumer(consumer, handler)

    def on_assign(c: Consumer, partitions: list[TopicPartition]) -> None:
        loop.on_rebalance(c, partitions)
        for tp in partitions:
            tp.offset = ends[tp.partition]
        c.assign(partitions)

    consumer.subscribe(
        [TOPIC], on_assign=on_assign, on_revoke=loop.on_rebalance, on_lost=loop.on_rebalance
    )
    try:
        deadline = time.monotonic() + 30
        while len(consumer.assignment()) < 3:
            assert time.monotonic() < deadline, "no partitions assigned within 30 s"
            loop.poll()

        published = {p: publish([(key, b"{}") for key in keys[p]], partition=p) for p in range(3)}
        deadline = time.monotonic() + 30
        while len(handler.handled) < 3 * PER_PARTITION:
            assert time.monotonic() < deadline, f"handled only {handler.handled}"
            loop.poll()
        for _ in range(4):  # nothing more arrives: no message is handled twice
            loop.poll()
    finally:
        consumer.close()

    assert handler.failed
    for p in range(3):
        mine = [(offset, key) for partition, offset, key in handler.handled if partition == p]
        assert mine == [
            (offset, key) for (_, offset), key in zip(published[p], keys[p], strict=True)
        ]
    # The failed message's partition waited: its next message came only after the retry.
    order = [key for _, _, key in handler.handled]
    assert order.index(keys[0][2]) < order.index(keys[0][3])
    assert loop.waits == {}
