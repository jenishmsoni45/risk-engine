"""PaySim's CSV as ledger transfers (design.md section 8; D32, D33, D40p, D51.2, D52).

`read_rows` parses and checks every row of the file. `load` keeps the TRANSFER and CASH_OUT rows
(D32) and turns each into a transfer the ledger accepts:
- `nameOrig` and `nameDest` become UUID v5s in one fixed namespace (D40p), so a name is always
  the same account;
- the amount becomes cents, rounded half to even, in CAD (D40p); a row that rounds to 0 cents is
  skipped and counted (D52f);
- step s is the hour [2026-01-01T00:00Z + (s - 1) h, + s h) (D51.2), and the hour's transfers are
  spaced evenly through it in file order, at millisecond resolution (D33).

Times strictly increase, which the replay's wait needs (D36, D52e): the loader refuses a file whose
steps go backwards, an hour with more transfers than milliseconds, and a transfer from an account
to itself, which the ledger would refuse. Everything is deterministic: one file always loads to the
same transfers.
"""

from __future__ import annotations

import csv
import uuid
from collections import Counter
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation
from pathlib import Path

from risk.features.compute import HOUR_MS
from risk.timestamps import epoch_ms

COLUMNS = (
    "step",
    "type",
    "amount",
    "nameOrig",
    "oldbalanceOrg",
    "newbalanceOrig",
    "nameDest",
    "oldbalanceDest",
    "newbalanceDest",
    "isFraud",
    "isFlaggedFraud",
)
TYPES = ("CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER")
LOADED_TYPES = ("TRANSFER", "CASH_OUT")  # D32
CURRENCY = "CAD"  # D40p
# D40p's one fixed namespace, derived from a name that says what it is for.
NAMESPACE = uuid.uuid5(
    uuid.NAMESPACE_URL, "https://github.com/jenishmsoni45/project-2-risk-engine#paysim"
)
STEP_1 = datetime(2026, 1, 1, tzinfo=UTC)  # D51.2
STEP_1_MS = epoch_ms(STEP_1)


class PaySimError(Exception):
    """The file isn't PaySim's format, or can't be replayed exactly (D52e)."""


@dataclass(frozen=True, slots=True)
class Row:
    """One CSV row, checked. `line` is its line in the file; the header is line 1."""

    line: int
    step: int
    type: str
    amount: Decimal
    name_orig: str
    name_dest: str
    is_fraud: bool
    is_flagged_fraud: bool


@dataclass(frozen=True, slots=True)
class Transfer:
    """A PaySim row as the transfer the replay posts and the offline pipeline reads."""

    line: int
    step: int
    type: str  # TRANSFER or CASH_OUT
    request_id: str  # the Idempotency-Key and requestId (D52d)
    sender: uuid.UUID
    receiver: uuid.UUID
    amount: int  # cents, at least 1
    time_ms: int  # epoch ms: requestedAt and createdAt (D40v)
    is_fraud: bool


@dataclass
class LoadStats:
    """What `load` read and skipped."""

    rows: int = 0
    by_type: Counter[str] = field(default_factory=Counter)
    zero_amount: int = 0  # loaded types only: rounded to 0 cents, skipped (D52f)
    transfers: int = 0


def cents(amount: Decimal) -> int:
    """Minor units, rounded half to even (D40p): 0.125 is 12 cents and 0.135 is 14."""
    return int((amount * 100).quantize(Decimal(1), rounding=ROUND_HALF_EVEN))


def account_id(name: str, namespace: uuid.UUID = NAMESPACE) -> uuid.UUID:
    return uuid.uuid5(namespace, name)


def request_id(step: int, line: int, namespace: uuid.UUID = NAMESPACE) -> str:
    """D52d: the row's step and line, so one file always gives the same keys."""
    return str(uuid.uuid5(namespace, f"paysim:{step}:{line}"))


def step_start_ms(step: int) -> int:
    return STEP_1_MS + (step - 1) * HOUR_MS


def hour_times(step: int, count: int) -> list[int]:
    """D33: `count` times spread evenly through the step's hour, in order, at millisecond
    resolution; strictly increasing while count is at most the hour's milliseconds."""
    if count > HOUR_MS:
        raise PaySimError(f"step {step} has {count} transfers, more than its hour's milliseconds")
    start = step_start_ms(step)
    return [start + i * HOUR_MS // count for i in range(count)]


def _flag(value: str, name: str, line: int) -> bool:
    if value not in ("0", "1"):
        raise PaySimError(f"line {line}: {name} must be 0 or 1, got {value!r}")
    return value == "1"


def _row(values: list[str], line: int) -> Row:
    if len(values) != len(COLUMNS):
        raise PaySimError(f"line {line}: {len(values)} columns, expected {len(COLUMNS)}")
    step_text, type_, amount_text, name_orig, _, _, name_dest, _, _, fraud, flagged = values
    if not step_text.isdigit() or int(step_text) < 1:
        raise PaySimError(f"line {line}: step must be a whole number from 1, got {step_text!r}")
    if type_ not in TYPES:
        raise PaySimError(f"line {line}: unknown type {type_!r}")
    try:
        amount = Decimal(amount_text)
    except InvalidOperation:
        raise PaySimError(f"line {line}: amount isn't a number: {amount_text!r}") from None
    if not amount.is_finite() or amount < 0:
        raise PaySimError(f"line {line}: amount must be a finite number from 0: {amount_text!r}")
    if not name_orig or not name_dest:
        raise PaySimError(f"line {line}: nameOrig and nameDest must not be empty")
    return Row(
        line=line,
        step=int(step_text),
        type=type_,
        amount=amount,
        name_orig=name_orig,
        name_dest=name_dest,
        is_fraud=_flag(fraud, "isFraud", line),
        is_flagged_fraud=_flag(flagged, "isFlaggedFraud", line),
    )


def read_rows(path: Path) -> Iterator[Row]:
    """Every row of a PaySim CSV, checked, in file order."""
    with path.open(newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if header is None or tuple(header) != COLUMNS:
            raise PaySimError(f"{path}: the header must be PaySim's columns {list(COLUMNS)}")
        for values in reader:
            yield _row(values, reader.line_num)


def _transfers(step: int, kept: list[tuple[Row, int]], namespace: uuid.UUID) -> Iterator[Transfer]:
    for (row, amount), time_ms in zip(kept, hour_times(step, len(kept)), strict=True):
        yield Transfer(
            line=row.line,
            step=row.step,
            type=row.type,
            request_id=request_id(row.step, row.line, namespace),
            sender=account_id(row.name_orig, namespace),
            receiver=account_id(row.name_dest, namespace),
            amount=amount,
            time_ms=time_ms,
            is_fraud=row.is_fraud,
        )


def transfers_from(
    rows: Iterable[Row], namespace: uuid.UUID = NAMESPACE, stats: LoadStats | None = None
) -> Iterator[Transfer]:
    """`load` on rows already read: the kept rows of each step, as transfers in time order.
    `stats`, when given, is filled in as rows are read."""
    stats = stats if stats is not None else LoadStats()
    step: int | None = None
    kept: list[tuple[Row, int]] = []
    for row in rows:
        stats.rows += 1
        stats.by_type[row.type] += 1
        if step is not None and row.step < step:
            raise PaySimError(f"line {row.line}: step {row.step} comes after step {step}")
        if row.step != step:
            if step is not None:
                yield from _transfers(step, kept, namespace)
            step, kept = row.step, []
        if row.type not in LOADED_TYPES:
            continue
        amount = cents(row.amount)
        if amount == 0:
            stats.zero_amount += 1
            continue
        if row.name_orig == row.name_dest:
            raise PaySimError(f"line {row.line}: {row.name_orig} sends to itself")
        kept.append((row, amount))
        stats.transfers += 1
    if step is not None:
        yield from _transfers(step, kept, namespace)


def load(
    path: Path, namespace: uuid.UUID = NAMESPACE, stats: LoadStats | None = None
) -> Iterator[Transfer]:
    """The file's TRANSFER and CASH_OUT rows as transfers, in time order (see the module)."""
    return transfers_from(read_rows(path), namespace, stats)
