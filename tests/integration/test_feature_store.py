"""The feature data in Redis (architecture.md section 4, design.md 3.2 to 3.4), on database 15 of
the stack's Redis: expiry (D40r, D45), trimming, the read range, and data in a format the module
never writes.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest
import redis
import redis.asyncio as aioredis

from risk import settings
from risk.features.compute import DecisionMembers, Movement
from risk.features.redis_store import (
    SET_EXPIRY_S,
    WATERMARK_KEY,
    CorruptFeatureDataError,
    FeatureStore,
    LedgerWriter,
    attempt_key,
    ledger_key,
    parse_members,
    parse_watermark,
)
from tests.feature_state import SCRATCH_DB, redis_url

T = 1_793_000_000_000
DAY_MS = 24 * 3600 * 1000
RETENTION_MS = DAY_MS + settings.lateness_margin_ms()
A, B = uuid.UUID(int=0xA), uuid.UUID(int=0xB)


def writer(db: redis.Redis) -> LedgerWriter:
    return LedgerWriter(db, settings.lateness_margin_ms())


async def _read(sender: uuid.UUID, receiver: uuid.UUID, t_ms: int) -> tuple[DecisionMembers, int]:
    client = aioredis.Redis.from_url(redis_url(SCRATCH_DB), socket_timeout=5)
    try:
        return await FeatureStore(client, settings.lateness_margin_ms()).read(
            sender, receiver, t_ms
        )
    finally:
        await client.aclose()


async def _record(sender: uuid.UUID, request_id: str, amount: int, t_ms: int) -> None:
    client = aioredis.Redis.from_url(redis_url(SCRATCH_DB), socket_timeout=5)
    try:
        store = FeatureStore(client, settings.lateness_margin_ms())
        await store.record_attempt(sender, request_id, amount, t_ms)
    finally:
        await client.aclose()


def read(sender: uuid.UUID, receiver: uuid.UUID, t_ms: int) -> tuple[DecisionMembers, int]:
    return asyncio.run(_read(sender, receiver, t_ms))


def record(sender: uuid.UUID, request_id: str, amount: int, t_ms: int) -> None:
    asyncio.run(_record(sender, request_id, amount, t_ms))


def test_sets_expire_after_25_hours_and_the_watermark_never(scratch: redis.Redis) -> None:
    scratch.flushdb()
    writer(scratch).apply(uuid.uuid4(), T, [Movement(A, "out", 5), Movement(B, "in", 5)])
    record(A, "r-1", 5, T)
    for key in (ledger_key(A, "out"), ledger_key(B, "in"), attempt_key(A)):
        assert SET_EXPIRY_S - 60 < scratch.ttl(key) <= SET_EXPIRY_S
    assert SET_EXPIRY_S == 25 * 3600
    # D45: the watermark has no expiry.
    assert scratch.ttl(WATERMARK_KEY) == -1


def test_the_watermark_only_rises_and_an_entry_with_no_movement_still_raises_it(
    scratch: redis.Redis,
) -> None:
    scratch.flushdb()
    w = writer(scratch)
    assert w.apply(uuid.uuid4(), T, []).watermark_ms == T
    assert w.apply(uuid.uuid4(), T - 5000, [Movement(A, "out", 1)]).watermark_ms == T
    assert int(scratch.get(WATERMARK_KEY) or 0) == T


def test_applying_trims_the_touched_sets_below_the_watermark_less_the_retention(
    scratch: redis.Redis,
) -> None:
    scratch.flushdb()
    w = writer(scratch)
    old, kept, newest = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    w.apply(old, T - RETENTION_MS - 1, [Movement(A, "out", 1)])
    w.apply(kept, T - RETENTION_MS, [Movement(A, "out", 2)])
    w.apply(newest, T, [Movement(A, "out", 3)])
    assert scratch.zrange(ledger_key(A, "out"), 0, -1) == [
        f"2:{kept}".encode(),
        f"3:{newest}".encode(),
    ]


def test_recording_an_attempt_trims_the_senders_older_attempts(scratch: redis.Redis) -> None:
    scratch.flushdb()
    record(A, "old", 1, T - RETENTION_MS - 1)
    record(A, "edge", 2, T - RETENTION_MS)
    record(A, "new", 3, T)
    record(A, "new", 3, T)  # again: no change
    assert scratch.zrange(attempt_key(A), 0, -1, withscores=True) == [
        (b"2:edge", float(T - RETENTION_MS)),
        (b"3:new", float(T)),
    ]


def test_the_read_takes_t_minus_24_h_and_stops_before_t(scratch: redis.Redis) -> None:
    scratch.flushdb()
    key = ledger_key(A, "out")
    scratch.zadd(key, {"1:before": T - DAY_MS - 1, "2:first": T - DAY_MS, "4:last": T - 1})
    scratch.zadd(key, {"8:at-t": T, "16:after": T + 1})
    members, watermark = read(A, B, T)
    assert watermark == 0  # D43: none applied yet
    assert [(m.time_ms, m.amount) for m in members.sender_out] == [(T - DAY_MS, 2), (T - 1, 4)]
    assert members.receiver_in == members.receiver_out == members.sender_in == []


def test_a_watermark_that_is_not_a_number_stops_applying_and_reading(scratch: redis.Redis) -> None:
    scratch.flushdb()
    scratch.set(WATERMARK_KEY, "soon")
    with pytest.raises(redis.ResponseError, match="wm is not a number"):
        writer(scratch).apply(uuid.uuid4(), T, [Movement(A, "out", 1)])
    with pytest.raises(CorruptFeatureDataError):
        read(A, B, T)


@pytest.mark.parametrize(
    "flat",
    [
        [b"no-colon", b"1"],
        [b"x:id", b"1"],
        [b"0:id", b"1"],
        [b"-5:id", b"1"],
        [b"5:", b"1"],
        [b"5:id", b"1.5"],
        [b"5:id", b"inf"],
        [b"5:id"],
        "5:id",
    ],
)
def test_a_member_in_a_foreign_format_is_refused(flat: object) -> None:
    with pytest.raises(CorruptFeatureDataError):
        parse_members(flat)


def test_watermarks_parse_and_a_foreign_one_is_refused() -> None:
    assert parse_watermark(None) == 0
    assert parse_watermark(b"1793000000000") == 1_793_000_000_000
    with pytest.raises(CorruptFeatureDataError):
        parse_watermark(b"1.5e12")
