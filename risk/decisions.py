"""The decision request (design.md 1.1) and the decision path (design.md section 4)."""

from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import redis.asyncio as aioredis
from psycopg_pool import AsyncConnectionPool

from risk import store
from risk.problems import FieldError
from risk.rules.ruleset import RuleSet
from risk.timestamps import epoch_ms, parse_offset_timestamp, truncate_to_ms

REQUEST_ID = re.compile(r"[\x21-\x7E]{1,255}")
# RFC 9562's 8-4-4-4-12 form; hex digits are case-insensitive on input.
CANONICAL_UUID = re.compile(r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}")
CURRENCY = re.compile(r"[A-Z]{3}")
MAX_AMOUNT = 2**63 - 1

WATERMARK_KEY = "wm"


class InvalidRequestError(Exception):
    def __init__(self, errors: list[FieldError]) -> None:
        super().__init__("; ".join(f"{e.field}: {e.message}" for e in errors))
        self.errors = errors


@dataclass(frozen=True)
class DecisionRequest:
    request_id: str
    from_account_id: uuid.UUID
    to_account_id: uuid.UUID
    amount: int
    currency: str
    requested_at: datetime  # UTC, truncated to the millisecond

    def same_transfer(self, other: store.StoredDecision) -> bool:
        """D11: the fields a repeated requestId must match. requestedAt is not compared."""
        return (
            self.from_account_id == other.from_account_id
            and self.to_account_id == other.to_account_id
            and self.amount == other.amount
            and self.currency == other.currency
        )


def parse_request(body: bytes) -> DecisionRequest:
    """The request of design.md 1.1, or InvalidRequestError naming every broken field."""
    try:
        document: Any = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        raise InvalidRequestError([FieldError("body", "must be a JSON object")]) from None
    if not isinstance(document, dict):
        raise InvalidRequestError([FieldError("body", "must be a JSON object")])

    errors: list[FieldError] = []

    def field(name: str) -> Any:
        if document.get(name) is None:
            errors.append(FieldError(name, "is required"))
        return document.get(name)

    request_id = field("requestId")
    if request_id is not None and not (
        isinstance(request_id, str) and REQUEST_ID.fullmatch(request_id)
    ):
        errors.append(FieldError("requestId", "must be 1 to 255 printable ASCII characters"))

    accounts: dict[str, uuid.UUID] = {}
    for name in ("fromAccountId", "toAccountId"):
        value = field(name)
        if value is None:
            continue
        if isinstance(value, str) and CANONICAL_UUID.fullmatch(value):
            accounts[name] = uuid.UUID(value)
        else:
            errors.append(FieldError(name, "must be a UUID in the 8-4-4-4-12 form"))
    if len(accounts) == 2 and accounts["fromAccountId"] == accounts["toAccountId"]:
        errors.append(FieldError("toAccountId", "must differ from fromAccountId"))

    amount = field("amount")
    # A JSON integer only: 100.0 and "100" are refused, and so is true, which Python reads as 1.
    if amount is not None and (
        not isinstance(amount, int) or isinstance(amount, bool) or not 1 <= amount <= MAX_AMOUNT
    ):
        errors.append(FieldError("amount", f"must be a JSON integer from 1 to {MAX_AMOUNT}"))

    currency = field("currency")
    if currency is not None and not (isinstance(currency, str) and CURRENCY.fullmatch(currency)):
        errors.append(FieldError("currency", "must be 3 uppercase letters"))

    requested_at_text = field("requestedAt")
    requested_at: datetime | None = None
    if requested_at_text is not None:
        try:
            requested_at = truncate_to_ms(parse_offset_timestamp(requested_at_text))
        except ValueError as exc:
            errors.append(FieldError("requestedAt", f"{exc} with an offset"))

    if errors or requested_at is None:
        raise InvalidRequestError(errors)
    return DecisionRequest(
        request_id=request_id,
        from_account_id=accounts["fromAccountId"],
        to_account_id=accounts["toAccountId"],
        amount=amount,
        currency=currency,
        requested_at=requested_at,
    )


@dataclass(frozen=True)
class Answer:
    """What a 200 carries, apart from onTimeout, which is always the current rule set's (D13)."""

    decision_id: uuid.UUID
    decision: str
    rule_set: str
    fired: list[str]
    replayed: bool


@dataclass(frozen=True)
class Conflict:
    """The requestId was decided for a different transfer (D11)."""


def answer_from(stored: store.StoredDecision) -> Answer:
    return Answer(
        decision_id=stored.decision_id,
        decision=stored.decision,
        rule_set=f"v{stored.rule_set_version}",
        fired=stored.fired,
        replayed=True,
    )


class CorruptWatermarkError(Exception):
    """`wm` in Redis isn't an integer, so no lag can be computed."""


async def read_watermark(redis: aioredis.Redis) -> int:
    """The latest createdAt the consumer applied, in epoch ms; 0 when there is none yet (D43)."""
    raw = await redis.get(WATERMARK_KEY)
    if raw is None:
        return 0
    try:
        return int(raw)
    except ValueError:
        raise CorruptWatermarkError(f"{WATERMARK_KEY} holds {raw!r}") from None


class DecisionEngine:
    """Steps 3 to 7 of design.md section 4. The deadline and the 503 are risk/api.py's."""

    def __init__(self, rules: RuleSet, pool: AsyncConnectionPool, redis: aioredis.Redis) -> None:
        self.rules = rules
        self.pool = pool
        self.redis = redis

    async def decide(self, request: DecisionRequest) -> Answer | Conflict:
        async with self.pool.connection() as conn:
            stored = await store.find(conn, request.request_id)
            if stored is not None:
                return answer_from(stored) if request.same_transfer(stored) else Conflict()

            # Phase 2 has no window features: the snapshot holds the amount alone (D43).
            features = {"amount": request.amount}
            lag_ms = epoch_ms(request.requested_at) - await read_watermark(self.redis)
            decision, fired = self.rules.evaluate(features)
            row = store.NewDecision(
                decision_id=uuid.uuid4(),
                request_id=request.request_id,
                from_account_id=request.from_account_id,
                to_account_id=request.to_account_id,
                amount=request.amount,
                currency=request.currency,
                requested_at=request.requested_at,
                decided_at=datetime.now(UTC),
                decision=decision,
                rule_set_version=self.rules.version,
                rule_set_hash=self.rules.sha256,
                fired=fired,
                features=features,
                feature_lag_ms=lag_ms,
            )
            if await store.insert(conn, row):
                return Answer(row.decision_id, decision, self.rules.name, fired, replayed=False)

            # A concurrent request with this requestId committed first: answer with its row (I1).
            winner = await store.find(conn, request.request_id)
            if winner is None:
                raise RuntimeError(f"request {request.request_id!r} conflicted but has no row")
            return answer_from(winner) if request.same_transfer(winner) else Conflict()
