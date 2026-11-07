"""The decision request (design.md 1.1) and the decision path (design.md section 4), with the
shadow score (2b, D39)."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from psycopg_pool import AsyncConnectionPool

from risk import store
from risk.features.compute import features_at
from risk.features.redis_store import FeatureStore
from risk.formats import CANONICAL_UUID, CURRENCY, IDEMPOTENCY_KEY, MAX_AMOUNT
from risk.problems import FieldError
from risk.rules.ruleset import RuleSet
from risk.shadow import Shadow
from risk.timestamps import epoch_ms, parse_offset_timestamp, truncate_to_ms


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
        isinstance(request_id, str) and IDEMPOTENCY_KEY.fullmatch(request_id)
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


class DecisionEngine:
    """Steps 3 to 8 of design.md section 4. The deadline and the 503 are risk/api.py's.

    `shadow` is None when shadow scoring is off (D57.5)."""

    def __init__(
        self,
        rules: RuleSet,
        pool: AsyncConnectionPool,
        features: FeatureStore,
        shadow: Shadow | None = None,
    ) -> None:
        self.rules = rules
        self.pool = pool
        self.features = features
        self.shadow = shadow

    async def decide(self, request: DecisionRequest) -> Answer | Conflict:
        """Steps 3 to 7: a stored decision, a 409, or a new decision row."""
        async with self.pool.connection() as conn:
            stored = await store.find(conn, request.request_id)
            if stored is not None:
                return answer_from(stored) if request.same_transfer(stored) else Conflict()

            # design.md 3.4: one script reads the five sets and the watermark; the shared module
            # turns them into the 31 features at t. The lag is negative when the stream is ahead.
            t_ms = epoch_ms(request.requested_at)
            members, watermark_ms = await self.features.read(
                request.from_account_id, request.to_account_id, t_ms
            )
            features = features_at(t_ms, request.amount, members)
            lag_ms = t_ms - watermark_ms
            decision, fired = self.rules.evaluate(features)
            # Step 6 (D39): the decision and fired are fixed above; the score is only stored, and
            # Shadow.score never raises, so the model can't change the answer (I8).
            score, model = None, None
            if self.shadow is not None:
                score, model = await self.shadow.score(features), self.shadow.model_name
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
                shadow_score=score,
                shadow_model=model,
            )
            if await store.insert(conn, row):
                return Answer(row.decision_id, decision, self.rules.name, fired, replayed=False)

            # A concurrent request with this requestId committed first: answer with its row (I1).
            winner = await store.find(conn, request.request_id)
            if winner is None:
                raise RuntimeError(f"request {request.request_id!r} conflicted but has no row")
            return answer_from(winner) if request.same_transfer(winner) else Conflict()

    async def record_attempt(self, request: DecisionRequest) -> None:
        """Step 8 (design.md 3.3): called once the new row has committed, never for a replay, so a
        retried requestId counts once (D18, D40g)."""
        await self.features.record_attempt(
            request.from_account_id,
            request.request_id,
            request.amount,
            epoch_ms(request.requested_at),
        )
