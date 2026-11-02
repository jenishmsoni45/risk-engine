"""The ledger event on ledger.entries, as the consumer reads it (design.md 1.2).

Parsing works on JSON, never on bytes, and ignores unknown fields. A missing decisionId reads as
null and a missing riskCheck as none (events from before tally's V7). Anything else that breaks the
format raises BadEventError, which the consumer stores in bad_events (Phase 3).
"""

from __future__ import annotations

import json
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from risk.formats import CANONICAL_UUID
from risk.timestamps import parse_event_timestamp, truncate_to_ms

KINDS = ("transfer", "entry", "reversal")


class BadEventError(Exception):
    pass


@dataclass(frozen=True)
class Line:
    id: int
    account_id: uuid.UUID
    amount: int
    currency: str


@dataclass(frozen=True)
class LedgerEvent:
    id: uuid.UUID
    kind: str
    reverses_id: uuid.UUID | None
    description: str | None
    effective_at: datetime  # UTC, truncated to the millisecond
    created_at: datetime  # UTC, truncated to the millisecond
    decision_id: uuid.UUID | None
    risk_check: str
    lines: tuple[Line, ...]


def _uuid(value: Any, name: str) -> uuid.UUID:
    if isinstance(value, str) and CANONICAL_UUID.fullmatch(value):
        return uuid.UUID(value)
    raise BadEventError(f"{name} must be a UUID")


def _optional_uuid(value: Any, name: str) -> uuid.UUID | None:
    return None if value is None else _uuid(value, name)


def _integer(value: Any, name: str) -> int:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    raise BadEventError(f"{name} must be an integer")


def _string(value: Any, name: str) -> str:
    if isinstance(value, str):
        return value
    raise BadEventError(f"{name} must be a string")


def _optional_string(value: Any, name: str) -> str | None:
    return None if value is None else _string(value, name)


def _timestamp(value: Any, name: str) -> datetime:
    try:
        return truncate_to_ms(parse_event_timestamp(value))
    except ValueError as exc:
        raise BadEventError(f"{name} {exc}") from None


def _line(value: Any, index: int) -> Line:
    if not isinstance(value, dict):
        raise BadEventError(f"lines[{index}] must be an object")
    amount = _integer(value.get("amount"), f"lines[{index}].amount")
    if amount == 0:
        raise BadEventError(f"lines[{index}].amount must not be zero")
    return Line(
        id=_integer(value.get("id"), f"lines[{index}].id"),
        account_id=_uuid(value.get("accountId"), f"lines[{index}].accountId"),
        amount=amount,
        currency=_string(value.get("currency"), f"lines[{index}].currency"),
    )


def parse_event(key: bytes | None, value: bytes) -> LedgerEvent:
    try:
        document: Any = json.loads(value)
    except (ValueError, UnicodeDecodeError):
        raise BadEventError("the value isn't JSON") from None
    if not isinstance(document, dict):
        raise BadEventError("the value must be a JSON object")

    entry_id = _uuid(document.get("id"), "id")
    if key is None or key.decode("utf-8", "replace") != document["id"]:
        raise BadEventError("the key must be the entry id")
    kind = document.get("kind")
    if kind not in KINDS:
        raise BadEventError(f"kind must be one of {KINDS}")

    raw_lines = document.get("lines")
    if not isinstance(raw_lines, list) or not raw_lines:
        raise BadEventError("lines must be a non-empty list")
    lines = tuple(_line(line, i) for i, line in enumerate(raw_lines))
    totals: defaultdict[str, int] = defaultdict(int)
    for line in lines:
        totals[line.currency] += line.amount
    unbalanced = sorted(currency for currency, total in totals.items() if total != 0)
    if unbalanced:
        raise BadEventError(f"lines don't sum to zero in {unbalanced}")

    return LedgerEvent(
        id=entry_id,
        kind=kind,
        reverses_id=_optional_uuid(document.get("reversesId"), "reversesId"),
        description=_optional_string(document.get("description"), "description"),
        effective_at=_timestamp(document.get("effectiveAt"), "effectiveAt"),
        created_at=_timestamp(document.get("createdAt"), "createdAt"),
        decision_id=_optional_uuid(document.get("decisionId"), "decisionId"),
        # Events from before tally's V7 have no risk fields (design.md 1.2).
        risk_check=_string(document.get("riskCheck", "none"), "riskCheck"),
        lines=lines,
    )
