"""Rebuilds the feature data in Redis from its sources (design.md 3.5, D40r, D48.1).

    make rebuild-features    (runs `python -m risk.features.rebuild` in the consumer's container)

Redis is never the source of truth: the ledger events are in `ledger.entries` and the attempts are
the decision log. The rebuild:

1. reads the watermark, if Redis still has one;
2. clears the feature keys: every `f:*` and `a:*` set, and `wm`;
3. replays `ledger.entries` through the consumer's own handler, from the offsets for
   `wm - 24 h - margin`, or from the start of each partition when there is no watermark, up to the
   end each partition had when the rebuild began; the running consumer applies what comes after;
4. records each sender's attempts from `decisions` with the decision API's own script: every row
   no older than the sender's latest `requested_at` - 24 h - margin.

Every write is idempotent and the watermark only rises, so the running consumer and decision API
may keep writing meanwhile. Until the rebuild ends, decisions see the features restored so far:
the failure table's "features read empty until make rebuild-features" (design.md section 7).

A failure of Redis, Postgres or Kafka stops the rebuild with a message; it can simply be run again.
"""

from __future__ import annotations

import logging
import sys
import time
import uuid
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Protocol

import psycopg
import redis
from confluent_kafka import Consumer, KafkaException, Message, TopicPartition

from risk import settings
from risk.features.consumer import (
    CALL_TIMEOUT_S,
    BadEvents,
    MessageHandler,
    Outcome,
    RetryLaterError,
)
from risk.features.redis_store import (
    RECORD_ATTEMPT_LUA,
    WATERMARK_KEY,
    LedgerWriter,
    attempt_call,
    parse_watermark,
    retention_ms,
)
from risk.settings import TOPIC
from risk.timestamps import epoch_ms

log = logging.getLogger(__name__)

FEATURE_KEY_PATTERNS = ("f:*", "a:*")
# Kafka's message timestamp is the publisher's clock and createdAt the ledger database's; starting
# a minute earlier covers a skew between the two. Replaying more changes nothing.
CLOCK_SKEW_MS = 60_000
BATCH = 500
# How long the replay waits for messages it knows are there before giving up.
STALL_S = 30.0
# Reading every sender's recent decisions is one query over the decision log.
ATTEMPTS_QUERY_TIMEOUT_MS = 60_000

LATEST_ATTEMPTS = """
select from_account_id, request_id, amount, requested_at
from (
    select from_account_id, request_id, amount, requested_at,
           max(requested_at) over (partition by from_account_id) as latest
    from decisions
) d
where requested_at >= latest - make_interval(secs => %s)
order by from_account_id, requested_at
"""


class RebuildError(Exception):
    pass


class OffsetLookup(Protocol):
    """The calls start_offsets makes on confluent_kafka.Consumer."""

    def offsets_for_times(
        self, partitions: list[TopicPartition], timeout: float = -1
    ) -> list[TopicPartition]: ...

    def get_watermark_offsets(
        self, partition: TopicPartition, timeout: float = -1, cached: bool = False
    ) -> tuple[int, int]: ...


def start_offsets(
    kafka: OffsetLookup, ends: dict[int, int], watermark_ms: int | None, retention: int
) -> dict[int, int]:
    """Where each partition's replay starts: its oldest kept message when there is no watermark
    (D48.1), else the first message published at or after wm - retention (less the clock skew),
    or its end when there is none."""
    if watermark_ms is None:
        return {
            p: kafka.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=CALL_TIMEOUT_S)[0]
            for p in ends
        }
    since = max(watermark_ms - retention - CLOCK_SKEW_MS, 0)
    found = kafka.offsets_for_times(
        [TopicPartition(TOPIC, p, since) for p in ends], timeout=CALL_TIMEOUT_S
    )
    return {tp.partition: tp.offset if tp.offset >= 0 else ends[tp.partition] for tp in found}


def clear(client: redis.Redis) -> int:
    """Deletes every feature key and the watermark; returns how many keys went."""
    removed = 0
    for pattern in FEATURE_KEY_PATTERNS:
        batch: list[bytes] = []
        for key in client.scan_iter(match=pattern, count=1000):
            batch.append(key)
            if len(batch) >= 1000:
                removed += int(client.unlink(*batch))
                batch = []
        if batch:
            removed += int(client.unlink(*batch))
    removed += int(client.unlink(WATERMARK_KEY))
    return removed


@dataclass
class Replay:
    starts: dict[int, int]
    ends: dict[int, int]
    outcomes: Counter[Outcome] = field(default_factory=Counter)


def replay(
    kafka: Consumer, handler: MessageHandler, starts: dict[int, int], ends: dict[int, int]
) -> Replay:
    """Hands every message from each partition's start up to its end to the handler, in the
    order Kafka delivers them. Messages past an end are left to the running consumer."""
    result = Replay(starts=starts, ends=ends)
    pending = {p for p in ends if starts[p] < ends[p]}
    if not pending:
        return result
    kafka.assign([TopicPartition(TOPIC, p, starts[p]) for p in sorted(pending)])
    last_progress = time.monotonic()
    while pending:
        messages: list[Message] = kafka.consume(num_messages=BATCH, timeout=1.0)
        if not messages:
            # A partition whose position reached its end without a message at end - 1 (a gap in
            # the log) is done too.
            for tp in kafka.position([TopicPartition(TOPIC, p) for p in pending]):
                if tp.offset >= ends[tp.partition]:
                    pending.discard(tp.partition)
            if pending and time.monotonic() - last_progress > STALL_S:
                raise RebuildError(f"no messages from partitions {sorted(pending)} for {STALL_S} s")
        for message in messages:
            error = message.error()
            if error is not None:
                if error.fatal():
                    raise KafkaException(error)
                log.warning("kafka: %s", error.str())
                continue
            partition, offset = message.partition(), message.offset()
            if partition is None or offset is None or partition not in pending:
                continue
            if offset >= ends[partition]:
                pending.discard(partition)
                continue
            result.outcomes[handler.handle(message)] += 1
            last_progress = time.monotonic()
            if offset + 1 >= ends[partition]:
                pending.discard(partition)
    return result


@dataclass(frozen=True)
class Attempt:
    sender: uuid.UUID
    request_id: str
    amount: int
    requested_ms: int


def latest_attempts(conn: psycopg.Connection, retention: int) -> Iterator[Attempt]:
    """Each sender's decisions no older than its latest requested_at - retention, in time order:
    what the decision API's own writes and trims leave (design.md 3.3)."""
    with conn.cursor(name="rebuild_attempts") as cursor:
        cursor.itersize = 5000
        cursor.execute(LATEST_ATTEMPTS, (retention / 1000,))
        for sender, request_id, amount, requested_at in cursor:
            yield Attempt(sender, request_id, amount, epoch_ms(requested_at))


def record(client: redis.Redis, attempts: Iterator[Attempt], retention: int) -> tuple[int, int]:
    """Records the attempts with the decision API's script; returns (attempts, senders)."""
    script = client.register_script(RECORD_ATTEMPT_LUA)
    count = 0
    senders: set[uuid.UUID] = set()
    pipe = client.pipeline(transaction=False)
    for attempt in attempts:
        keys, args = attempt_call(
            attempt.sender, attempt.request_id, attempt.amount, attempt.requested_ms, retention
        )
        script(keys=keys, args=args, client=pipe)
        count += 1
        senders.add(attempt.sender)
        if count % BATCH == 0:
            pipe.execute()
    pipe.execute()
    return count, len(senders)


def topic_ends(kafka: Consumer) -> dict[int, int]:
    metadata = kafka.list_topics(TOPIC, timeout=CALL_TIMEOUT_S).topics[TOPIC]
    if metadata.error is not None:
        raise RebuildError(f"{TOPIC}: {metadata.error}")
    return {
        p: kafka.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=CALL_TIMEOUT_S)[1]
        for p in sorted(metadata.partitions)
    }


def rebuild(
    client: redis.Redis, kafka: Consumer, database_url: str, margin_ms: int
) -> dict[str, object]:
    retention = retention_ms(margin_ms)
    raw = client.get(WATERMARK_KEY)
    watermark = None if raw is None else parse_watermark(raw)
    ends = topic_ends(kafka)
    starts = start_offsets(kafka, ends, watermark, retention)
    cleared = clear(client)
    log.info("cleared %d keys; replaying %s from %s to %s", cleared, TOPIC, starts, ends)
    handler = MessageHandler(LedgerWriter(client, margin_ms), BadEvents(database_url))
    replayed = replay(kafka, handler, starts, ends)
    with psycopg.connect(
        database_url,
        connect_timeout=CALL_TIMEOUT_S,
        options=f"-c statement_timeout={ATTEMPTS_QUERY_TIMEOUT_MS}",
    ) as conn:
        attempts, senders = record(client, latest_attempts(conn, retention), retention)
    return {
        "watermark_before": watermark,
        "keys_cleared": cleared,
        "starts": starts,
        "ends": ends,
        "events": dict(replayed.outcomes),
        "attempts": attempts,
        "senders": senders,
        "watermark_after": parse_watermark(client.get(WATERMARK_KEY)),
    }


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    client = redis.Redis.from_url(
        settings.redis_url(), socket_timeout=CALL_TIMEOUT_S, socket_connect_timeout=CALL_TIMEOUT_S
    )
    kafka = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap(),
            # Never committed: the replay reads by assignment and leaves the group's offsets alone.
            "group.id": f"risk-features-rebuild-{uuid.uuid4().hex}",
            "enable.auto.commit": False,
        }
    )
    try:
        summary = rebuild(client, kafka, settings.database_url(), settings.lateness_margin_ms())
    except (RebuildError, RetryLaterError, KafkaException, redis.RedisError, psycopg.Error) as exc:
        log.error("rebuild failed, nothing is lost; run it again: %r", exc)
        return 1
    finally:
        kafka.close()
        client.close()
    for name, value in summary.items():
        print(f"{name}: {value}")  # noqa: T201
    return 0


if __name__ == "__main__":
    sys.exit(main())
