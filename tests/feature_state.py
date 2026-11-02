"""The feature code's own paths, run against Redis from the tests.

Messages go through the consumer's MessageHandler (parse, net, the apply script) and features are
read through the decision API's FeatureStore and the shared module, exactly as in the services;
only Kafka is left out, so a test chooses the delivery order. The property tests use database 15
of the stack's Redis, which no service touches, so they can't disturb the stack or be disturbed.
FakeSource stands in for Kafka when a test drives the consumer's loop itself.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Literal

import redis
import redis.asyncio as aioredis
from confluent_kafka import KafkaError, KafkaException, Message, TopicPartition
from prometheus_client import REGISTRY

from risk import settings
from risk.features.compute import features_at
from risk.features.consumer import MessageHandler, Outcome
from risk.features.redis_store import FeatureStore, LedgerWriter
from risk.settings import TOPIC
from tests.ledger_events import Event
from tests.oracle import OracleAttempt

REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
STACK_DB = 0
SCRATCH_DB = 15


def redis_url(db: int) -> str:
    return f"redis://{REDIS_HOST}:{REDIS_PORT}/{db}"


def client(db: int) -> redis.Redis:
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=db, socket_timeout=5)


def sample(name: str, **labels: str) -> float:
    """A metric of this process, 0 when it hasn't been recorded yet."""
    return REGISTRY.get_sample_value(name, labels) or 0.0


class FakeSource:
    """Stands in for confluent_kafka.Consumer in the consumer's loop: hands out the batches
    given, one per consume(), and records every other call."""

    def __init__(self, *batches: list[Message]) -> None:
        self.batches = list(batches)
        self.calls: list[tuple[str, object]] = []
        self.fail_commit = False

    def consume(self, num_messages: int = 1, timeout: float = -1) -> list[Message]:
        return self.batches.pop(0) if self.batches else []

    def seek(self, partition: TopicPartition) -> None:
        self.calls.append(("seek", (partition.partition, partition.offset)))

    def pause(self, partitions: list[TopicPartition]) -> None:
        self.calls.append(("pause", [p.partition for p in partitions]))

    def resume(self, partitions: list[TopicPartition]) -> None:
        self.calls.append(("resume", [p.partition for p in partitions]))

    def commit(
        self, *, offsets: list[TopicPartition], asynchronous: Literal[False]
    ) -> list[TopicPartition]:
        self.calls.append(("commit", [(p.partition, p.offset) for p in offsets]))
        if self.fail_commit:
            raise KafkaException(KafkaError(KafkaError._TIMED_OUT))
        return offsets


class NoBadEvents:
    """For logs that hold no bad event: storing one fails the test."""

    def store(self, message: Message, error: str) -> None:
        raise AssertionError(f"unexpected bad event: {error}")


def message(event: Event, offset: int = 0, partition: int = 0) -> Message:
    return Message(
        topic=TOPIC, partition=partition, offset=offset, key=event.key(), value=event.value()
    )


def deliver(db_client: redis.Redis, log: Sequence[Event], order: Iterable[int]) -> list[Outcome]:
    """Hands log[i] for each i of order to the consumer's handler, with the default margin."""
    handler = MessageHandler(LedgerWriter(db_client, settings.lateness_margin_ms()), NoBadEvents())
    return [handler.handle(message(log[i], offset)) for offset, i in enumerate(order)]


@dataclass(frozen=True)
class Query:
    sender: uuid.UUID
    receiver: uuid.UUID
    amount: int
    t_ms: int


async def _read(db: int, queries: Sequence[Query]) -> list[tuple[dict[str, int], int]]:
    async_client = aioredis.Redis.from_url(redis_url(db), socket_timeout=5)
    try:
        store = FeatureStore(async_client, settings.lateness_margin_ms())
        results = []
        for q in queries:
            members, watermark = await store.read(q.sender, q.receiver, q.t_ms)
            results.append((features_at(q.t_ms, q.amount, members), watermark))
        return results
    finally:
        await async_client.aclose()


def read(db: int, queries: Sequence[Query]) -> list[tuple[dict[str, int], int]]:
    """Each query's 31 features and the watermark, read the way the decision API reads them."""
    return asyncio.run(_read(db, queries))


async def _record(db: int, attempts: Sequence[OracleAttempt]) -> None:
    async_client = aioredis.Redis.from_url(redis_url(db), socket_timeout=5)
    try:
        store = FeatureStore(async_client, settings.lateness_margin_ms())
        for a in attempts:
            await store.record_attempt(a.sender, a.request_id, a.amount, a.time_ms)
    finally:
        await async_client.aclose()


def record_attempts(db: int, attempts: Sequence[OracleAttempt]) -> None:
    """Records each attempt, in the order given, the way the decision API does after a commit."""
    asyncio.run(_record(db, attempts))
