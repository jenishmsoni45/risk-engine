"""Where make rebuild-features starts replaying ledger.entries (design.md 3.5, D48.1): from the
offsets for wm - 24 h - margin when Redis still has a watermark, and from the start of each
partition when it doesn't. The replay itself is checked against the stack in
tests/adversarial/test_rebuild.py.
"""

from __future__ import annotations

from confluent_kafka import TopicPartition

from risk.features import rebuild
from risk.features.redis_store import retention_ms
from risk.settings import TOPIC

RETENTION = retention_ms(300_000)


class FakeKafka:
    """Each partition's oldest kept offset, and the offset offsets_for_times answers per
    partition (-1 when no message is that new)."""

    def __init__(self, lows: dict[int, int], by_time: dict[int, int]) -> None:
        self.lows = lows
        self.by_time = by_time
        self.asked: list[tuple[int, int]] = []

    def offsets_for_times(
        self, partitions: list[TopicPartition], timeout: float = -1
    ) -> list[TopicPartition]:
        self.asked += [(tp.partition, tp.offset) for tp in partitions]
        return [
            TopicPartition(TOPIC, tp.partition, self.by_time[tp.partition]) for tp in partitions
        ]

    def get_watermark_offsets(
        self, partition: TopicPartition, timeout: float = -1, cached: bool = False
    ) -> tuple[int, int]:
        return self.lows[partition.partition], 10**6


ENDS = {0: 50, 1: 60, 2: 70}


def test_without_a_watermark_every_partition_replays_from_its_oldest_kept_message() -> None:
    kafka = FakeKafka(lows={0: 0, 1: 12, 2: 3}, by_time={})
    assert rebuild.start_offsets(kafka, ENDS, None, RETENTION) == {0: 0, 1: 12, 2: 3}
    assert kafka.asked == []


def test_with_a_watermark_the_replay_starts_at_the_offsets_for_wm_less_the_retention() -> None:
    wm = 1_793_000_000_000
    kafka = FakeKafka(lows={}, by_time={0: 7, 1: 8, 2: 9})
    assert rebuild.start_offsets(kafka, ENDS, wm, RETENTION) == {0: 7, 1: 8, 2: 9}
    # 24 h + 5 min behind the watermark, and a minute more for the clocks' skew.
    since = wm - 24 * 3600 * 1000 - 300_000 - 60_000
    assert kafka.asked == [(0, since), (1, since), (2, since)]


def test_a_partition_with_nothing_that_new_has_nothing_to_replay() -> None:
    kafka = FakeKafka(lows={}, by_time={0: -1, 1: 8, 2: -1})
    assert rebuild.start_offsets(kafka, ENDS, 1_793_000_000_000, RETENTION) == {0: 50, 1: 8, 2: 70}
