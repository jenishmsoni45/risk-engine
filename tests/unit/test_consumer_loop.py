"""The consumer's loop and handler (design.md 3.2 and section 7), with a fake Kafka source.

Offsets are committed after the writes for the messages they cover (D40h). A message that fails
because Redis or Postgres does rewinds and pauses its own partition only; the others keep
flowing, and the failed message is read again after a growing delay.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from typing import Literal

import pytest
import redis
from confluent_kafka import KafkaError, KafkaException, Message, TopicPartition

from risk.features import consumer
from risk.features.compute import Movement
from risk.features.consumer import (
    FIRST_RETRY_S,
    MAX_RETRY_S,
    BadEvents,
    FeatureConsumer,
    MessageHandler,
    Outcome,
    RetryLaterError,
)
from risk.features.redis_store import Applied
from risk.settings import TOPIC
from tests.feature_state import FakeSource, sample
from tests.ledger_events import entry, transfer

A = uuid.UUID("00000000-0000-4000-8000-00000000000a")
B = uuid.UUID("00000000-0000-4000-8000-00000000000b")


def msg(partition: int, offset: int, value: bytes | None = b"{}") -> Message:
    return Message(topic=TOPIC, partition=partition, offset=offset, key=b"k", value=value)


Position = tuple[int, int]


class ScriptedHandler:
    """Answers each (partition, offset) as scripted: a dependency failure, an outcome, or
    applied."""

    def __init__(
        self,
        outcomes: dict[Position, Outcome] | None = None,
        failures: dict[Position, str] | None = None,
    ) -> None:
        self.outcomes = outcomes or {}
        self.failures = failures or {}
        self.handled: list[Position] = []

    def handle(self, message: Message) -> Outcome:
        position = (message.partition() or 0, message.offset() or 0)
        self.handled.append(position)
        if position in self.failures:
            raise RetryLaterError(self.failures[position])
        return self.outcomes.get(position, "applied")


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def test_offsets_are_committed_after_the_writes_they_cover() -> None:
    source = FakeSource([msg(0, 5), msg(1, 9), msg(0, 6)])
    handler = ScriptedHandler()
    seen_at_commit: list[list[tuple[int, int]]] = []
    fc = FeatureConsumer(
        source,
        handler,
        before_commit=lambda offsets: seen_at_commit.append(list(handler.handled)),
    )
    fc.poll()
    assert seen_at_commit == [[(0, 5), (1, 9), (0, 6)]]
    assert source.calls == [("commit", [(0, 7), (1, 10)])]


def test_every_outcome_moves_the_offset_on() -> None:
    outcomes: dict[Position, Outcome] = {(0, 1): "bad", (0, 2): "late", (0, 3): "reversal"}
    source = FakeSource([msg(0, 1), msg(0, 2), msg(0, 3)])
    before = {o: sample("risk_consumer_events_total", outcome=o) for o in ("bad", "late")}
    FeatureConsumer(source, ScriptedHandler(outcomes)).poll()
    assert source.calls == [("commit", [(0, 4)])]
    for outcome in ("bad", "late"):
        assert sample("risk_consumer_events_total", outcome=outcome) == before[outcome] + 1


def test_a_failure_rewinds_and_pauses_only_its_partition() -> None:
    source = FakeSource([msg(0, 1), msg(1, 1), msg(0, 2), msg(1, 2), msg(0, 3)])
    handler = ScriptedHandler(failures={(0, 2): "postgres"})
    before = sample("risk_consumer_retries_total", dependency="postgres")
    FeatureConsumer(source, handler, clock=Clock()).poll()
    # Partition 0 stops at the failed message; its later message isn't handled out of order.
    assert handler.handled == [(0, 1), (1, 1), (0, 2), (1, 2)]
    assert source.calls == [
        ("seek", (0, 2)),
        ("pause", [0]),
        ("commit", [(0, 2), (1, 3)]),
    ]
    assert sample("risk_consumer_retries_total", dependency="postgres") == before + 1


def test_the_failed_message_is_read_again_after_a_growing_delay() -> None:
    clock = Clock()
    source = FakeSource(
        [msg(0, 4)],
        [],
        [msg(0, 4)],
        [msg(0, 4), msg(0, 5)],
    )
    handler = ScriptedHandler(failures={(0, 4): "redis"})
    fc = FeatureConsumer(source, handler, clock=clock)

    fc.poll()  # fails: waits FIRST_RETRY_S
    assert fc.waits[(TOPIC, 0)].delay_s == FIRST_RETRY_S
    source.calls.clear()
    fc.poll()  # too early: nothing is resumed
    assert source.calls == []

    clock.now += FIRST_RETRY_S
    fc.poll()  # resumed at the failed message, which fails again: the delay doubles
    assert source.calls[:2] == [("seek", (0, 4)), ("resume", [0])]
    assert fc.waits[(TOPIC, 0)].delay_s == 2 * FIRST_RETRY_S

    handler.failures.clear()  # the dependency is back
    clock.now += 2 * FIRST_RETRY_S
    source.calls.clear()
    fc.poll()
    assert source.calls == [("seek", (0, 4)), ("resume", [0]), ("commit", [(0, 6)])]
    assert fc.waits == {}


def test_the_delay_stops_growing_at_the_maximum() -> None:
    clock = Clock()
    source = FakeSource()
    fc = FeatureConsumer(source, ScriptedHandler(failures={(0, 1): "redis"}), clock=clock)
    for _ in range(12):
        source.batches.append([msg(0, 1)])
        fc.poll()
        clock.now += MAX_RETRY_S
    assert fc.waits[(TOPIC, 0)].delay_s == MAX_RETRY_S


def test_a_rebalance_drops_the_waits_of_its_partitions() -> None:
    handler = ScriptedHandler(failures={(0, 1): "redis", (1, 1): "redis"})
    fc = FeatureConsumer(FakeSource([msg(0, 1), msg(1, 1)]), handler, clock=Clock())
    fc.poll()
    fc.on_rebalance(None, [TopicPartition(TOPIC, 0)])
    assert list(fc.waits) == [(TOPIC, 1)]


def test_a_failed_commit_is_counted_and_a_later_one_covers_it() -> None:
    source = FakeSource([msg(2, 7)], [msg(2, 8)])
    source.fail_commit = True
    before = sample("risk_consumer_commit_failures_total")
    fc = FeatureConsumer(source, ScriptedHandler())
    fc.poll()
    source.fail_commit = False
    fc.poll()
    assert sample("risk_consumer_commit_failures_total") == before + 1
    assert source.calls == [("commit", [(2, 8)]), ("commit", [(2, 9)])]


def test_a_kafka_error_is_skipped_and_a_fatal_one_stops_the_consumer() -> None:
    harmless = Message(error=KafkaError(KafkaError._TRANSPORT))
    source = FakeSource(
        [harmless, msg(0, 1)], [Message(error=KafkaError(KafkaError._FATAL, fatal=True))]
    )
    fc = FeatureConsumer(source, ScriptedHandler())
    fc.poll()
    assert source.calls == [("commit", [(0, 2)])]
    with pytest.raises(KafkaException):
        fc.poll()


# --- MessageHandler --------------------------------------------------------------------------


APPLIED = Applied(applied=True, watermark_ms=42)


class FakeWriter:
    def __init__(self, result: Applied | Exception = APPLIED) -> None:
        self.result = result
        self.calls: list[tuple[uuid.UUID, int, list[Movement]]] = []

    def apply(self, entry_id: uuid.UUID, created_ms: int, movements: Sequence[Movement]) -> Applied:
        self.calls.append((entry_id, created_ms, list(movements)))
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


class FakeBadEvents:
    def __init__(self) -> None:
        self.stored: list[tuple[int | None, str]] = []

    def store(self, message: Message, error: str) -> None:
        self.stored.append((message.offset(), error))


def test_a_valid_entry_is_netted_and_applied_at_its_created_at() -> None:
    event = entry([(A, -100), (B, 70), (A, 30)], 1_793_000_000_123_456)
    writer, bad = FakeWriter(Applied(True, 1_793_000_000_123)), FakeBadEvents()
    message = Message(topic=TOPIC, partition=0, offset=3, key=event.key(), value=event.value())
    assert MessageHandler(writer, bad).handle(message) == "applied"
    assert writer.calls == [
        (event.id, 1_793_000_000_123, [Movement(A, "out", 70), Movement(B, "in", 70)])
    ]
    assert bad.stored == []
    assert sample("risk_consumer_watermark_ms") == 1_793_000_000_123


def test_a_late_entry_is_reported_late() -> None:
    event = transfer(A, B, 5, 1_000_000)
    message = Message(topic=TOPIC, partition=0, offset=3, key=event.key(), value=event.value())
    assert MessageHandler(FakeWriter(Applied(False, 9)), FakeBadEvents()).handle(message) == "late"


def test_a_reversal_is_skipped_without_touching_redis() -> None:
    event = entry([(A, 100), (B, -100)], 1_000_000, kind="reversal")
    writer = FakeWriter()
    message = Message(topic=TOPIC, partition=0, offset=3, key=event.key(), value=event.value())
    assert MessageHandler(writer, FakeBadEvents()).handle(message) == "reversal"
    assert writer.calls == []


@pytest.mark.parametrize(
    ("value", "error"),
    [(b"not json", "isn't JSON"), (None, "has no value"), (b'{"id": 1}', "id must be a UUID")],
    ids=["not JSON", "tombstone", "invalid"],
)
def test_a_bad_event_is_stored_with_its_error_and_not_applied(
    value: bytes | None, error: str
) -> None:
    writer, bad = FakeWriter(), FakeBadEvents()
    assert MessageHandler(writer, bad).handle(msg(0, 11, value)) == "bad"
    assert len(bad.stored) == 1
    assert bad.stored[0][0] == 11
    assert error in bad.stored[0][1]
    assert writer.calls == []


def test_a_redis_failure_asks_for_the_message_again() -> None:
    event = transfer(A, B, 5, 1_000_000)
    message = Message(topic=TOPIC, partition=0, offset=3, key=event.key(), value=event.value())
    handler = MessageHandler(FakeWriter(redis.ConnectionError("down")), FakeBadEvents())
    with pytest.raises(RetryLaterError) as raised:
        handler.handle(message)
    assert raised.value.dependency == "redis"


def test_a_postgres_failure_while_storing_a_bad_event_asks_for_it_again() -> None:
    # Port 1 on the loopback refuses connections at once.
    bad_events = BadEvents("postgresql://nobody:dummy@127.0.0.1:1/nothing")
    with pytest.raises(RetryLaterError) as raised:
        MessageHandler(FakeWriter(), bad_events).handle(msg(0, 1, b"not json"))
    assert raised.value.dependency == "postgres"


def test_the_consumer_reads_from_the_oldest_kept_event_and_commits_by_hand() -> None:
    config = consumer.kafka_settings("kafka:9092")
    assert config["group.id"] == consumer.GROUP_ID
    assert config["enable.auto.commit"] is False
    assert config["auto.offset.reset"] == "earliest"


def test_the_group_and_its_start_come_from_the_settings() -> None:
    config = consumer.kafka_settings("kafka:9092", "risk-features-e2e", "latest")
    assert config["group.id"] == "risk-features-e2e"
    assert config["auto.offset.reset"] == "latest"


class FakeOffsets:
    """The offsets start_at_end reads and commits: partition -> committed offset (-1001 for none,
    as the client reports it) and partition -> the topic's end."""

    def __init__(self, committed: dict[int, int], ends: dict[int, int]) -> None:
        self._committed = committed
        self.ends = ends
        self.commits: list[list[tuple[int, int]]] = []

    def committed(
        self, partitions: list[TopicPartition], timeout: float = -1
    ) -> list[TopicPartition]:
        return [
            TopicPartition(TOPIC, p.partition, self._committed[p.partition]) for p in partitions
        ]

    def get_watermark_offsets(
        self, partition: TopicPartition, timeout: float = -1, cached: bool = False
    ) -> tuple[int, int]:
        return 0, self.ends[partition.partition]

    def commit(
        self, *, offsets: list[TopicPartition], asynchronous: Literal[False]
    ) -> list[TopicPartition]:
        self.commits.append([(o.partition, o.offset) for o in offsets])
        return offsets


def test_starting_at_the_end_commits_the_end_of_each_partition_never_committed() -> None:
    kafka = FakeOffsets(committed={0: -1001, 1: 7, 2: -1001}, ends={0: 40, 1: 50, 2: 60})
    partitions = [TopicPartition(TOPIC, p) for p in range(3)]

    committed = consumer.start_at_end(kafka, partitions)

    # Partition 1 already has an offset and keeps it; the others start at their current end.
    assert [(tp.partition, tp.offset) for tp in committed] == [(0, 40), (2, 60)]
    assert kafka.commits == [[(0, 40), (2, 60)]]


def test_starting_at_the_end_commits_nothing_when_every_partition_has_an_offset() -> None:
    kafka = FakeOffsets(committed={0: 3, 1: 4, 2: 5}, ends={0: 9, 1: 9, 2: 9})
    assert consumer.start_at_end(kafka, [TopicPartition(TOPIC, p) for p in range(3)]) == []
    assert consumer.start_at_end(kafka, []) == []
    assert kafka.commits == []
