"""The outbox relay (design.md 1.3, D40k), as tally's OutboxRelay works.

Every poll it locks up to a batch of unpublished rows (SKIP LOCKED, so several relays never take
the same row), sends them keyed by entry id, waits for every ack, then marks them published, all
in one transaction. A batch that fails rolls back and is sent again on the next poll: delivery is
at least once, and the consumer dedupes on the entry id. The value is the stored jsonb as text,
exactly what tally publishes.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid

from confluent_kafka import KafkaError, Message, Producer
from prometheus_client import Counter
from psycopg_pool import AsyncConnectionPool

from risk.settings import TOPIC

log = logging.getLogger(__name__)

PUBLISHED = Counter("stub_outbox_published", "Outbox rows published to ledger.entries")
FAILED_BATCHES = Counter(
    "stub_outbox_failed_batches", "Outbox batches rolled back, sent again later"
)

# tally's producer settings: an ack from every in-sync replica, idempotent retries, and a send
# that succeeds or fails within the 4 s the relay waits for its acks.
ACK_TIMEOUT_S = 4.0
# With a poll interval of 0 the relay waits for a posting to wake it; this retries a failed
# batch when no posting comes.
IDLE_RETRY_S = 1.0

LOCK_UNPUBLISHED = """
select id, entry_id, payload::text
from stub.outbox
where published_at is null
order by id
limit %s
for update skip locked
"""

MARK_PUBLISHED = "update stub.outbox set published_at = now() where id = any(%s)"


def producer_config(bootstrap: str) -> dict[str, str | int | bool]:
    return {
        "bootstrap.servers": bootstrap,
        "acks": "all",
        "enable.idempotence": True,
        "delivery.timeout.ms": 3000,
        "request.timeout.ms": 2000,
    }


class PublishError(Exception):
    pass


class Relay:
    def __init__(
        self, pool: AsyncConnectionPool, producer: Producer, poll_s: float, batch: int
    ) -> None:
        self.pool = pool
        self.producer = producer
        self.poll_s = poll_s
        self.batch = batch
        self._wake = asyncio.Event()

    def wake(self) -> None:
        """Called after a posting commits; only an interval of 0 listens (publish at once)."""
        self._wake.set()

    async def run(self) -> None:
        while True:
            # Cleared before the batch, so a posting that commits during it wakes the next one.
            self._wake.clear()
            try:
                published = await self.publish_batch()
            except Exception:
                # Logged, not ignored: the rows stay unpublished and the next poll retries them.
                FAILED_BATCHES.inc()
                log.warning("outbox batch rolled back; its rows are retried", exc_info=True)
                published = 0
            if published == self.batch:
                continue  # more may be waiting
            if self.poll_s > 0:
                await asyncio.sleep(self.poll_s)
            else:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._wake.wait(), IDLE_RETRY_S)

    async def publish_batch(self) -> int:
        async with self.pool.connection() as conn, conn.transaction():
            cursor = await conn.execute(LOCK_UNPUBLISHED, (self.batch,))
            rows: list[tuple[int, uuid.UUID, str]] = await cursor.fetchall()
            if not rows:
                return 0
            await asyncio.to_thread(self._send, rows)
            await conn.execute(MARK_PUBLISHED, ([row_id for row_id, _, _ in rows],))
        PUBLISHED.inc(len(rows))
        return len(rows)

    def _send(self, rows: list[tuple[int, uuid.UUID, str]]) -> None:
        """Every send goes out before any ack is awaited; raises unless every row is acked."""
        failures: list[str] = []

        def on_delivery(error: KafkaError | None, _message: Message) -> None:
            if error is not None:
                failures.append(str(error))

        for _, entry_id, payload in rows:
            self.producer.produce(
                TOPIC, key=str(entry_id).encode(), value=payload.encode(), on_delivery=on_delivery
            )
        unacked = self.producer.flush(ACK_TIMEOUT_S)
        if unacked or failures:
            raise PublishError(f"{unacked} unacked, failures: {failures[:3]}")
