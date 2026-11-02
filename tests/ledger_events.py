"""Ledger events for the feature tests: tally's event format (design.md 1.2), with the ground truth
the oracle reads, and the Hypothesis strategies that generate them.

createdAt is spelled with 0, 3, 6 or 9 fraction digits, as the consumer must accept; the oracle
gets the time the text spells, truncated to the millisecond, without parsing anything.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from hypothesis import strategies as st

from tests.oracle import OracleEntry, OracleLine

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
MS_US = 1000
SECOND_US = 1000 * MS_US
MINUTE_US = 60 * SECOND_US
HOUR_US = 60 * MINUTE_US
DAY_US = 24 * HOUR_US

# A few accounts, so that entries share accounts and windows hold several members.
ACCOUNTS = tuple(uuid.UUID(f"5e1f0000-0000-4000-8000-{n:012d}") for n in range(1, 5))


def spell(time_us: int, digits: int) -> str:
    """createdAt as tally writes it, with 0, 3, 6 or 9 fraction digits and a Z."""
    moment = EPOCH + timedelta(microseconds=time_us)
    text = moment.strftime("%Y-%m-%dT%H:%M:%S")
    fraction = f"{moment.microsecond:06d}"
    if digits == 0:
        return f"{text}Z"
    if digits in (3, 6):
        return f"{text}.{fraction[:digits]}Z"
    return f"{text}.{fraction}987Z"  # nanoseconds, which the consumer drops


def spelled_ms(time_us: int, digits: int) -> int:
    """The millisecond the spelled text names."""
    if digits == 0:
        return time_us // SECOND_US * 1000
    return time_us // MS_US


@dataclass(frozen=True)
class Event:
    id: uuid.UUID
    kind: str  # transfer, entry or reversal
    time_us: int
    lines: tuple[OracleLine, ...]
    digits: int = 3
    reverses_id: uuid.UUID | None = field(default=None)

    @property
    def time_ms(self) -> int:
        return spelled_ms(self.time_us, self.digits)

    @property
    def created_at(self) -> str:
        return spell(self.time_us, self.digits)

    def oracle(self) -> OracleEntry:
        return OracleEntry(id=self.id, kind=self.kind, time_ms=self.time_ms, lines=self.lines)

    def document(self) -> dict[str, Any]:
        return {
            "id": str(self.id),
            "kind": self.kind,
            "reversesId": str(self.reverses_id or uuid.uuid4())
            if self.kind == "reversal"
            else None,
            "description": None,
            "effectiveAt": self.created_at,
            "createdAt": self.created_at,
            "decisionId": None,
            "riskCheck": "none",
            "lines": [
                {
                    "id": number,
                    "accountId": str(line.account),
                    "amount": line.amount,
                    "currency": "CAD",
                }
                for number, line in enumerate(self.lines, start=1)
            ],
        }

    def key(self) -> bytes:
        return str(self.id).encode()

    def value(self) -> bytes:
        return json.dumps(self.document()).encode()


def transfer(
    sender: uuid.UUID, receiver: uuid.UUID, amount: int, time_us: int, *, digits: int = 3
) -> Event:
    return Event(
        id=uuid.uuid4(),
        kind="transfer",
        time_us=time_us,
        lines=(OracleLine(sender, -amount), OracleLine(receiver, amount)),
        digits=digits,
    )


def entry(lines: Sequence[tuple[uuid.UUID, int]], time_us: int, *, kind: str = "entry") -> Event:
    return Event(
        id=uuid.uuid4(),
        kind=kind,
        time_us=time_us,
        lines=tuple(OracleLine(account, amount) for account, amount in lines),
    )


def now_us() -> int:
    return (datetime.now(UTC) - EPOCH) // timedelta(microseconds=1)


# --- Hypothesis strategies -------------------------------------------------------------------

amounts = st.one_of(st.integers(1, 1_000_000), st.integers(1, 2**62))


@st.composite
def lines_for(draw: st.DrawFn, kind: str) -> tuple[OracleLine, ...]:
    if kind == "transfer":
        sender, receiver = draw(
            st.lists(st.sampled_from(ACCOUNTS), min_size=2, max_size=2, unique=True)
        )
        amount = draw(amounts)
        return (OracleLine(sender, -amount), OracleLine(receiver, amount))
    # An entry or a reversal: debit and credit pairs, possibly on one account, so that an
    # account's lines can partly or wholly cancel (-100 and +30, or -50 and +50).
    pairs = draw(
        st.lists(
            st.tuples(st.sampled_from(ACCOUNTS), st.sampled_from(ACCOUNTS), amounts),
            min_size=1,
            max_size=3,
        )
    )
    lines = [
        line
        for debit, credit, amount in pairs
        for line in (OracleLine(debit, -amount), OracleLine(credit, amount))
    ]
    return tuple(draw(st.permutations(lines)))


@st.composite
def events(draw: st.DrawFn, times_us: st.SearchStrategy[int]) -> Event:
    kind = draw(st.sampled_from(("transfer", "transfer", "entry", "reversal")))
    return Event(
        id=draw(st.uuids()),
        kind=kind,
        time_us=draw(times_us),
        lines=draw(lines_for(kind)),
        digits=draw(st.sampled_from((0, 3, 6, 9))),
    )


def event_logs(
    times_us: st.SearchStrategy[int], max_size: int = 20
) -> st.SearchStrategy[list[Event]]:
    return st.lists(events(times_us), min_size=1, max_size=max_size, unique_by=lambda e: e.id)


@st.composite
def deliveries(draw: st.DrawFn, count: int) -> list[int]:
    """Indexes of a log in delivery order: every entry at least once, some again, shuffled."""
    repeats = draw(st.lists(st.integers(0, count - 1), max_size=count))
    order: list[int] = draw(st.permutations([*range(count), *repeats]))
    return order
