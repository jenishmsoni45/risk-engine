"""The feature data in Redis (architecture.md section 4, design.md 3.2 to 3.4).

Three scripts, each one atomic step:
- APPLY (the consumer): an entry's members, the watermark and the trimming, or nothing at all for
  an event older than watermark - 24 h - margin (D15).
- RECORD_ATTEMPT (the decision API): one attempt, and the trimming of the sender's attempts.
- READ (the decision API): the five sets of a decision with scores in [t - 24 h, t), and `wm`.

The scripts only store and fetch members; counting and summing is risk/features/compute.py's
(D40y). Members are `{amount}:{id}` with the amount first, because a requestId may hold a colon.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import redis
import redis.asyncio as aioredis

from risk.features.compute import (
    LONGEST_WINDOW_MS,
    DecisionMembers,
    Direction,
    Member,
    Movement,
    read_range,
)

WATERMARK_KEY = "wm"
# Memory hygiene only (D40r): longer than any window plus the margin. `wm` never expires (D45).
SET_EXPIRY_S = 25 * 3600

APPLY_LUA = """
-- KEYS[1]: wm. KEYS[2..]: the sets that get this entry's members.
-- ARGV[1]: createdAt (epoch ms). ARGV[2]: retention, 24 h + margin (ms). ARGV[3]: set expiry (s).
-- ARGV[4..]: the member for each of KEYS[2..], in order.
-- Returns {1, wm} once applied, or {0, wm} when createdAt < wm - retention: late, nothing written.
local created = tonumber(ARGV[1])
local retention = tonumber(ARGV[2])
local current = redis.call('GET', KEYS[1])
local wm = nil
if current then
  wm = tonumber(current)
  if not wm then
    return redis.error_reply('wm is not a number: ' .. current)
  end
  if created < wm - retention then
    return {0, current}
  end
end
if not wm or created > wm then
  wm = created
  redis.call('SET', KEYS[1], ARGV[1])
end
local below = '(' .. string.format('%d', wm - retention)
for i = 2, #KEYS do
  redis.call('ZADD', KEYS[i], 'NX', ARGV[1], ARGV[i + 2])
  redis.call('ZREMRANGEBYSCORE', KEYS[i], '-inf', below)
  redis.call('EXPIRE', KEYS[i], ARGV[3])
end
return {1, string.format('%d', wm)}
"""

RECORD_ATTEMPT_LUA = """
-- KEYS[1]: the sender's attempts. ARGV[1]: requestedAt (epoch ms). ARGV[2]: the member.
-- ARGV[3]: the trim point, requestedAt - retention (ms). ARGV[4]: set expiry (s).
redis.call('ZADD', KEYS[1], 'NX', ARGV[1], ARGV[2])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', '(' .. ARGV[3])
redis.call('EXPIRE', KEYS[1], ARGV[4])
return 1
"""

READ_LUA = """
-- KEYS[1]: wm. KEYS[2..6]: sender out, sender in, sender attempts, receiver in, receiver out.
-- ARGV[1]: t - 24 h, included. ARGV[2]: t, excluded. Members with their scores; no arithmetic.
local reply = {redis.call('GET', KEYS[1])}
for i = 2, #KEYS do
  reply[i] = redis.call('ZRANGEBYSCORE', KEYS[i], ARGV[1], '(' .. ARGV[2], 'WITHSCORES')
end
return reply
"""


class CorruptFeatureDataError(Exception):
    """Redis holds a value in a format this module never writes."""


def ledger_key(account_id: uuid.UUID, direction: Direction) -> str:
    return f"f:{account_id}:{direction}"


def attempt_key(account_id: uuid.UUID) -> str:
    return f"a:{account_id}"


def member(amount: int, ident: str) -> str:
    return f"{amount}:{ident}"


def retention_ms(margin_ms: int) -> int:
    """How far behind the watermark an event may be and still count (D15, D40c)."""
    return LONGEST_WINDOW_MS + margin_ms


def parse_watermark(raw: Any) -> int:
    """`wm` in epoch ms; 0 when no event has been applied yet (D43)."""
    if raw is None:
        return 0
    try:
        return int(raw)
    except (TypeError, ValueError):
        raise CorruptFeatureDataError(f"{WATERMARK_KEY} holds {raw!r}") from None


def parse_members(flat: Any) -> list[Member]:
    """A ZRANGEBYSCORE ... WITHSCORES reply: member, score, member, score, ..."""
    if not isinstance(flat, list) or len(flat) % 2:
        raise CorruptFeatureDataError(f"not a member and score list: {flat!r}")
    members = []
    for raw_member, raw_score in zip(flat[::2], flat[1::2], strict=True):
        corrupt = CorruptFeatureDataError(f"member {raw_member!r} with score {raw_score!r}")
        if not isinstance(raw_member, bytes) or not isinstance(raw_score, bytes):
            raise corrupt
        amount_text, sep, ident = raw_member.partition(b":")
        try:
            # Scores are whole milliseconds; a double holds them exactly (below 2^53).
            score = float(raw_score)
            amount = int(amount_text)
        except ValueError:
            raise corrupt from None
        if not sep or not ident or amount < 1 or not score.is_integer():
            raise corrupt
        members.append(Member(time_ms=int(score), amount=amount))
    return members


@dataclass(frozen=True)
class Applied:
    applied: bool  # False: older than wm - 24 h - margin, so dropped (D15)
    watermark_ms: int


class LedgerWriter:
    """The consumer's writes (design.md 3.2, steps 3 and 4)."""

    def __init__(self, client: redis.Redis, margin_ms: int) -> None:
        self._apply = client.register_script(APPLY_LUA)
        self.retention_ms = retention_ms(margin_ms)

    def apply(self, entry_id: uuid.UUID, created_ms: int, movements: Sequence[Movement]) -> Applied:
        """Adds the entry's members, raises the watermark and trims the touched sets, in one
        script. An entry with no movements still raises the watermark."""
        keys = [WATERMARK_KEY, *(ledger_key(m.account_id, m.direction) for m in movements)]
        args: list[int | str] = [
            created_ms,
            self.retention_ms,
            SET_EXPIRY_S,
            *(member(m.amount, str(entry_id)) for m in movements),
        ]
        reply = self._apply(keys=keys, args=args)
        if not isinstance(reply, list) or len(reply) != 2 or reply[0] not in (0, 1):
            raise CorruptFeatureDataError(f"unexpected reply from the apply script: {reply!r}")
        return Applied(applied=reply[0] == 1, watermark_ms=parse_watermark(reply[1]))


class FeatureStore:
    """The decision API's reads and attempt writes (design.md 3.3 and 3.4)."""

    def __init__(self, client: aioredis.Redis, margin_ms: int) -> None:
        self._read = client.register_script(READ_LUA)
        self._record_attempt = client.register_script(RECORD_ATTEMPT_LUA)
        self.retention_ms = retention_ms(margin_ms)

    async def read(
        self, sender: uuid.UUID, receiver: uuid.UUID, t_ms: int
    ) -> tuple[DecisionMembers, int]:
        """The members a decision at t needs, and the watermark."""
        low, high = read_range(t_ms)
        keys = [
            WATERMARK_KEY,
            ledger_key(sender, "out"),
            ledger_key(sender, "in"),
            attempt_key(sender),
            ledger_key(receiver, "in"),
            ledger_key(receiver, "out"),
        ]
        reply = await self._read(keys=keys, args=[low, high])
        if not isinstance(reply, list) or len(reply) != len(keys):
            raise CorruptFeatureDataError(f"unexpected reply from the read script: {reply!r}")
        members = DecisionMembers(
            sender_out=parse_members(reply[1]),
            sender_in=parse_members(reply[2]),
            sender_attempts=parse_members(reply[3]),
            receiver_in=parse_members(reply[4]),
            receiver_out=parse_members(reply[5]),
        )
        return members, parse_watermark(reply[0])

    async def record_attempt(
        self, sender: uuid.UUID, request_id: str, amount: int, requested_ms: int
    ) -> None:
        """Adds `{amount}:{requestId}` at requestedAt and trims the sender's attempts below
        requestedAt - 24 h - margin. Adding the same attempt again changes nothing."""
        await self._record_attempt(
            keys=[attempt_key(sender)],
            args=[
                requested_ms,
                member(amount, request_id),
                requested_ms - self.retention_ms,
                SET_EXPIRY_S,
            ],
        )
