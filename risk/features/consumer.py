"""The feature consumer (design.md 3.2): reads ledger.entries and keeps the window members and the
watermark in Redis.

For each message: parse and validate it (design.md 1.2) and store a bad one in bad_events (D40i);
skip a reversal (D17); net the entry per account; then one Redis script either drops a late event
(D15) or adds the members, raises the watermark and trims. Offsets are committed after the writes
they cover (D40h), so a crash replays messages, which changes nothing (I2).

A message that fails because Redis or Postgres does is read again: its partition is rewound to it
and paused for a growing delay, while the other partitions keep flowing (design.md section 7).

Run with `python -m risk.features.consumer`; GET /metrics is served on CONSUMER_METRICS_PORT.
"""

from __future__ import annotations

import logging
import signal
import threading
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from types import FrameType
from typing import Any, Literal, Protocol

import psycopg
import redis
from confluent_kafka import Consumer, KafkaException, Message, TopicPartition
from prometheus_client import Counter, Gauge, start_http_server

from risk import settings
from risk.features.compute import Movement, net_entry
from risk.features.events import BadEventError, parse_event
from risk.features.redis_store import Applied, LedgerWriter
from risk.settings import TOPIC
from risk.timestamps import epoch_ms

log = logging.getLogger(__name__)

GROUP_ID = "risk-features"
BATCH_SIZE = 500
POLL_TIMEOUT_S = 0.5
# A partition whose message failed is read again after this delay, doubling up to the maximum.
FIRST_RETRY_S = 0.1
MAX_RETRY_S = 5.0
# Each Redis call and each Postgres connection gives up after this long, and is retried.
CALL_TIMEOUT_S = 5

Outcome = Literal["applied", "reversal", "late", "bad"]
Partition = tuple[str, int]

EVENTS = Counter(
    "risk_consumer_events",
    "Messages handled from ledger.entries, by outcome: applied, reversal (skipped, D17), late "
    "(dropped, D15) or bad (stored in bad_events). A message read again after a crash counts "
    "again.",
    ["outcome"],
)
RETRIES = Counter(
    "risk_consumer_retries",
    "Messages to be read again because a dependency failed, by dependency: redis or postgres",
    ["dependency"],
)
COMMIT_FAILURES = Counter(
    "risk_consumer_commit_failures",
    "Offset commits that failed; a later commit covers the same messages",
)
WATERMARK = Gauge(
    "risk_consumer_watermark_ms", "The watermark after the latest applied event, in epoch ms"
)

INSERT_BAD_EVENT = """
insert into bad_events (topic, kafka_partition, kafka_offset, message_key, message_value, error)
values (%s, %s, %s, %s, %s, %s)
on conflict (topic, kafka_partition, kafka_offset) do nothing
"""


class RetryLaterError(Exception):
    """A dependency failed; the message must be read again."""

    def __init__(self, dependency: str) -> None:
        super().__init__(dependency)
        self.dependency = dependency


class BadEvents:
    """bad_events in Postgres (D40i). Bad events are rare, so each one opens its own connection,
    whose wait is bounded; a replayed message is stored once (its topic, partition and offset are
    unique)."""

    def __init__(self, database_url: str) -> None:
        self.database_url = database_url

    def store(self, message: Message, error: str) -> None:
        try:
            with psycopg.connect(
                self.database_url,
                autocommit=True,
                connect_timeout=CALL_TIMEOUT_S,
                options=f"-c statement_timeout={CALL_TIMEOUT_S * 1000}",
            ) as conn:
                conn.execute(
                    INSERT_BAD_EVENT,
                    (
                        message.topic(),
                        message.partition(),
                        message.offset(),
                        message.key(),
                        message.value(),
                        error,
                    ),
                )
        except psycopg.Error as exc:
            log.warning("can't store a bad event: %r", exc)
            raise RetryLaterError("postgres") from exc


class Writer(Protocol):
    """LedgerWriter's one call."""

    def apply(
        self, entry_id: uuid.UUID, created_ms: int, movements: Sequence[Movement]
    ) -> Applied: ...


class BadEventSink(Protocol):
    """BadEvents's one call."""

    def store(self, message: Message, error: str) -> None: ...


class MessageHandler:
    """design.md 3.2, steps 1 to 4, for one message."""

    def __init__(self, writer: Writer, bad_events: BadEventSink) -> None:
        self.writer = writer
        self.bad_events = bad_events

    def handle(self, message: Message) -> Outcome:
        value = message.value()
        try:
            if value is None:
                raise BadEventError("the message has no value")
            event = parse_event(message.key(), value)
        except BadEventError as exc:
            log.warning(
                "bad event at %s[%s]@%s: %s",
                message.topic(),
                message.partition(),
                message.offset(),
                exc,
            )
            self.bad_events.store(message, str(exc))
            return "bad"
        if event.kind == "reversal":
            return "reversal"
        try:
            applied = self.writer.apply(event.id, epoch_ms(event.created_at), net_entry(event))
        except redis.RedisError as exc:
            log.warning("can't apply entry %s: %r", event.id, exc)
            raise RetryLaterError("redis") from exc
        if not applied.applied:
            return "late"
        WATERMARK.set(applied.watermark_ms)
        return "applied"


class Source(Protocol):
    """The calls the loop makes on confluent_kafka.Consumer."""

    def consume(self, num_messages: int = 1, timeout: float = -1) -> list[Message]: ...

    def seek(self, partition: TopicPartition) -> None: ...

    def pause(self, partitions: list[TopicPartition]) -> None: ...

    def resume(self, partitions: list[TopicPartition]) -> None: ...

    def commit(
        self, *, offsets: list[TopicPartition], asynchronous: Literal[False]
    ) -> list[TopicPartition]: ...


class Handler(Protocol):
    def handle(self, message: Message) -> Outcome: ...


@dataclass
class _Wait:
    offset: int  # the message that failed, which is read again first
    delay_s: float
    until: float | None  # when to resume; None once resumed and retrying


class FeatureConsumer:
    """The consume, handle and commit loop. `before_commit` is called with the offsets after
    their messages' writes and before the commit; the crash test uses it."""

    def __init__(
        self,
        source: Source,
        handler: Handler,
        *,
        before_commit: Callable[[list[TopicPartition]], None] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.source = source
        self.handler = handler
        self.before_commit = before_commit
        self.clock = clock
        self.waits: dict[Partition, _Wait] = {}

    def run(self, stop: threading.Event) -> None:
        while not stop.is_set():
            self.poll()

    def poll(self) -> None:
        """Waits up to POLL_TIMEOUT_S for one message, then takes what is already fetched, up to
        BATCH_SIZE in all, without waiting (D51.1). librdkafka waits out the timeout unless a
        batch fills, so asking for the whole batch at once kept a lone event waiting."""
        self._resume_due()
        messages = self.source.consume(num_messages=1, timeout=POLL_TIMEOUT_S)
        if messages:
            messages += self.source.consume(num_messages=BATCH_SIZE - 1, timeout=0)
        self.process(messages)

    def process(self, messages: list[Message]) -> None:
        commit: dict[Partition, int] = {}
        for message in messages:
            error = message.error()
            if error is not None:
                if error.fatal():
                    raise KafkaException(error)
                log.warning("kafka: %s", error.str())
                continue
            topic, partition, offset = message.topic(), message.partition(), message.offset()
            if topic is None or partition is None or offset is None:
                raise KafkaException(f"a fetched message without a position: {message!r}")
            key = (topic, partition)
            wait = self.waits.get(key)
            if wait is not None and wait.until is not None:
                continue  # paused and rewound: this message is read again after the wait
            try:
                outcome = self.handler.handle(message)
            except RetryLaterError as exc:
                RETRIES.labels(exc.dependency).inc()
                self._wait(key, offset, wait)
                continue
            EVENTS.labels(outcome).inc()
            commit[key] = offset + 1
            if wait is not None:
                del self.waits[key]
        if commit:
            offsets = [TopicPartition(t, p, o) for (t, p), o in sorted(commit.items())]
            if self.before_commit is not None:
                self.before_commit(offsets)
            try:
                self.source.commit(offsets=offsets, asynchronous=False)
            except KafkaException as exc:
                COMMIT_FAILURES.inc()
                log.warning("offset commit failed, a later one covers it: %r", exc)

    def on_rebalance(self, _consumer: Any, partitions: list[TopicPartition]) -> None:
        """A partition assigned, revoked or lost starts again from its committed offset, so a
        wait on it no longer applies."""
        for tp in partitions:
            self.waits.pop((tp.topic, tp.partition), None)

    def _wait(self, key: Partition, offset: int, previous: _Wait | None) -> None:
        delay = FIRST_RETRY_S if previous is None else min(previous.delay_s * 2, MAX_RETRY_S)
        # Rewinding drops the messages already fetched after this one; a failed seek raises,
        # and the restart reads from the committed offset, which is never past this message.
        position = TopicPartition(key[0], key[1], offset)
        self.source.seek(position)
        self.source.pause([position])
        self.waits[key] = _Wait(offset=offset, delay_s=delay, until=self.clock() + delay)
        log.warning("%s[%s]@%s is read again in %.1f s", key[0], key[1], offset, delay)

    def _resume_due(self) -> None:
        now = self.clock()
        due = [
            (key, wait)
            for key, wait in self.waits.items()
            if wait.until is not None and wait.until <= now
        ]
        for key, wait in due:
            position = TopicPartition(key[0], key[1], wait.offset)
            self.source.seek(position)
            self.source.resume([position])
            wait.until = None


@dataclass(frozen=True)
class ConsumerConfig:
    bootstrap: str
    redis_url: str
    database_url: str
    margin_ms: int
    metrics_port: int
    group: str = GROUP_ID
    start: str = "earliest"  # or "latest" (D48.6)

    @classmethod
    def from_env(cls) -> ConsumerConfig:
        return cls(
            bootstrap=settings.kafka_bootstrap(),
            redis_url=settings.redis_url(),
            database_url=settings.database_url(),
            margin_ms=settings.lateness_margin_ms(),
            metrics_port=settings.consumer_metrics_port(),
            group=settings.consumer_group(),
            start=settings.consumer_start(),
        )


def kafka_settings(
    bootstrap: str, group: str = GROUP_ID, start: str = "earliest"
) -> dict[str, str | int | bool]:
    return {
        "bootstrap.servers": bootstrap,
        "group.id": group,
        # Offsets are committed by hand, after the writes they cover (D40h).
        "enable.auto.commit": False,
        # By default a new consumer group starts at the oldest event kept, so no delayed event is
        # skipped; make e2e starts a new group at the end of tally's topic instead (D48.6).
        "auto.offset.reset": start,
    }


class Offsets(Protocol):
    """The calls start_at_end makes on confluent_kafka.Consumer."""

    def committed(
        self, partitions: list[TopicPartition], timeout: float = -1
    ) -> list[TopicPartition]: ...

    def get_watermark_offsets(
        self, partition: TopicPartition, timeout: float = -1, cached: bool = False
    ) -> tuple[int, int]: ...

    def commit(
        self, *, offsets: list[TopicPartition], asynchronous: Literal[False]
    ) -> list[TopicPartition]: ...


def start_at_end(kafka: Offsets, partitions: list[TopicPartition]) -> list[TopicPartition]:
    """CONSUMER_START=latest (D48.6), on assignment: each partition the group has never committed
    gets the topic's current end committed at once, before any message is read. A restart then
    resumes from there instead of jumping to a later end and skipping what arrived meanwhile, and
    make e2e can tell from the group's offsets when the consumer is in place. Returns the
    offsets committed."""
    if not partitions:
        return []
    fresh = []
    for tp in kafka.committed(partitions, timeout=CALL_TIMEOUT_S):
        if tp.offset < 0:
            position = TopicPartition(tp.topic, tp.partition)
            _, end = kafka.get_watermark_offsets(position, timeout=CALL_TIMEOUT_S)
            fresh.append(TopicPartition(tp.topic, tp.partition, end))
    if fresh:
        kafka.commit(offsets=fresh, asynchronous=False)
        log.info("starting at the end: %s", [(tp.partition, tp.offset) for tp in fresh])
    return fresh


def run(
    config: ConsumerConfig,
    stop: threading.Event,
    *,
    before_commit: Callable[[list[TopicPartition]], None] | None = None,
    kafka_overrides: Mapping[str, str | int | bool] | None = None,
) -> None:
    # Listens on every interface inside the container; Compose publishes it on 127.0.0.1 only.
    server, thread = start_http_server(config.metrics_port)
    log.info("consumer metrics on port %d", config.metrics_port)
    client = redis.Redis.from_url(
        config.redis_url, socket_timeout=CALL_TIMEOUT_S, socket_connect_timeout=CALL_TIMEOUT_S
    )
    kafka = Consumer(
        {
            **kafka_settings(config.bootstrap, config.group, config.start),
            **(kafka_overrides or {}),
        }
    )
    try:
        handler = MessageHandler(
            LedgerWriter(client, config.margin_ms), BadEvents(config.database_url)
        )
        consumer = FeatureConsumer(kafka, handler, before_commit=before_commit)

        def on_assign(assigned: Consumer, partitions: list[TopicPartition]) -> None:
            consumer.on_rebalance(assigned, partitions)
            if config.start == "latest":
                # Committed before the assignment takes effect, so reading starts there.
                start_at_end(assigned, partitions)

        kafka.subscribe(
            [TOPIC],
            on_assign=on_assign,
            on_revoke=consumer.on_rebalance,
            on_lost=consumer.on_rebalance,
        )
        consumer.run(stop)
    finally:
        kafka.close()
        client.close()
        server.shutdown()
        thread.join()
        log.info("consumer stopped")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    stop = threading.Event()

    def on_signal(signum: int, _frame: FrameType | None) -> None:
        log.info("received signal %d, stopping", signum)
        stop.set()

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)
    run(ConsumerConfig.from_env(), stop)


if __name__ == "__main__":
    main()
