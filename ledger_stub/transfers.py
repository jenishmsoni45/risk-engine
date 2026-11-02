"""tally's transfer request and the entry it posts (design.md 1.3, D24).

Validation follows tally's TransferRequest: two different account UUIDs, a positive whole amount,
a 3-letter currency, an optional description of at most 500 characters and an optional
effectiveAt. The entry is tally's JournalEntry, fields in the same order.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from risk.decisions import CANONICAL_UUID, CURRENCY, MAX_AMOUNT
from risk.problems import FieldError
from risk.timestamps import format_instant, parse_offset_timestamp

IDEMPOTENCY_KEY = re.compile(r"[\x21-\x7E]{1,255}")
MAX_DESCRIPTION = 500

# JournalEntry's and JournalLine's record components, in order.
ENTRY_FIELDS = (
    "id",
    "kind",
    "reversesId",
    "description",
    "effectiveAt",
    "createdAt",
    "decisionId",
    "riskCheck",
    "lines",
)
LINE_FIELDS = ("id", "accountId", "amount", "currency")


class UnreadableTransferError(Exception):
    """The body isn't a JSON object (tally: "Failed to read request")."""


class InvalidTransferError(Exception):
    def __init__(self, errors: list[FieldError]) -> None:
        super().__init__("; ".join(f"{e.field}: {e.message}" for e in errors))
        self.errors = errors


@dataclass(frozen=True)
class TransferRequest:
    from_account_id: uuid.UUID
    to_account_id: uuid.UUID
    amount: int
    currency: str
    description: str | None
    effective_at: datetime | None

    def request_hash(self) -> str:
        """SHA-256 of the operation and the request as read, like tally's RequestHash: field
        order, an absent or null field and the offset's spelling don't change it."""
        canonical = json.dumps(
            {
                "fromAccountId": str(self.from_account_id),
                "toAccountId": str(self.to_account_id),
                "amount": self.amount,
                "currency": self.currency,
                "description": self.description,
                "effectiveAt": format_instant(self.effective_at) if self.effective_at else None,
            },
            separators=(",", ":"),
            ensure_ascii=False,
        )
        return hashlib.sha256(f"transfer\n{canonical}".encode()).hexdigest()


def utf16_length(text: str) -> int:
    """Java's String.length(), which @Size counts: a character outside the BMP counts twice."""
    return len(text.encode("utf-16-le")) // 2


def parse_transfer(key: str | None, body: bytes) -> tuple[str, TransferRequest]:
    try:
        document: Any = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        raise UnreadableTransferError from None
    if not isinstance(document, dict):
        raise UnreadableTransferError

    errors: list[FieldError] = []
    if key is None:
        errors.append(FieldError("Idempotency-Key", "is required"))
    elif not IDEMPOTENCY_KEY.fullmatch(key):
        errors.append(FieldError("Idempotency-Key", "must be 1 to 255 printable ASCII characters"))

    accounts: dict[str, uuid.UUID] = {}
    for name in ("fromAccountId", "toAccountId"):
        value = document.get(name)
        if value is None:
            errors.append(FieldError(name, "must not be null"))
        elif isinstance(value, str) and CANONICAL_UUID.fullmatch(value):
            accounts[name] = uuid.UUID(value)
        else:
            errors.append(FieldError(name, "must be a UUID"))
    if len(accounts) == 2 and accounts["fromAccountId"] == accounts["toAccountId"]:
        errors.append(FieldError("toAccountDifferent", "must differ from fromAccountId"))

    amount = document.get("amount")
    if amount is None:
        errors.append(FieldError("amount", "must not be null"))
    elif not isinstance(amount, int) or isinstance(amount, bool) or amount > MAX_AMOUNT:
        errors.append(FieldError("amount", "must be a whole number of cents"))
    elif amount <= 0:
        errors.append(FieldError("amount", "must be greater than 0"))

    currency = document.get("currency")
    if not isinstance(currency, str) or not currency.strip():
        errors.append(FieldError("currency", "must not be blank"))
    elif not CURRENCY.fullmatch(currency):
        errors.append(FieldError("currency", "must be 3 uppercase letters"))

    description = document.get("description")
    if description is not None and not isinstance(description, str):
        errors.append(FieldError("description", "must be a string"))
    elif description is not None and utf16_length(description) > MAX_DESCRIPTION:
        errors.append(FieldError("description", f"size must be between 0 and {MAX_DESCRIPTION}"))

    effective_at: datetime | None = None
    if document.get("effectiveAt") is not None:
        try:
            effective_at = parse_offset_timestamp(document["effectiveAt"])
        except ValueError as exc:
            errors.append(FieldError("effectiveAt", str(exc)))

    # The isinstance checks only narrow the types: an error is already recorded for any field
    # that fails them.
    if errors or key is None or not isinstance(amount, int) or not isinstance(currency, str):
        raise InvalidTransferError(errors)
    return key, TransferRequest(
        from_account_id=accounts["fromAccountId"],
        to_account_id=accounts["toAccountId"],
        amount=amount,
        currency=currency,
        description=description,
        effective_at=effective_at,
    )


def transfer_entry(
    *,
    entry_id: uuid.UUID,
    request: TransferRequest,
    created_at: datetime,
    line_ids: tuple[int, int],
    risk_check: str,
    decision_id: uuid.UUID | None,
) -> dict[str, Any]:
    """The entry tally posts for a transfer: the sender's line, then the receiver's. A missing
    effectiveAt becomes the posting's createdAt, as in tally."""
    return {
        "id": str(entry_id),
        "kind": "transfer",
        "reversesId": None,
        "description": request.description,
        "effectiveAt": format_instant(request.effective_at or created_at),
        "createdAt": format_instant(created_at),
        "decisionId": str(decision_id) if decision_id else None,
        "riskCheck": risk_check,
        "lines": [
            {
                "id": line_ids[0],
                "accountId": str(request.from_account_id),
                "amount": -request.amount,
                "currency": request.currency,
            },
            {
                "id": line_ids[1],
                "accountId": str(request.to_account_id),
                "amount": request.amount,
                "currency": request.currency,
            },
        ],
    }


def in_record_order(entry: dict[str, Any]) -> dict[str, Any]:
    """A saved entry (jsonb keeps no key order) with tally's field order, as its replay reads the
    saved body back into a JournalEntry."""
    ordered = {name: entry.get(name) for name in ENTRY_FIELDS}
    ordered["lines"] = [{name: line.get(name) for name in LINE_FIELDS} for line in entry["lines"]]
    return ordered
