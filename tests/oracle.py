"""The brute-force oracle (design.md 3.5): the 31 features recomputed from the raw event log and
the attempts, sharing no code with risk/features/, so it can check that module.

It works on ground truth (times as whole milliseconds, lines as written), never on parsed events
or on Redis, and does everything the slow, obvious way: for every query it walks every entry and
every line. Duplicates are removed by id, so a log delivered twice gives the same answer.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Protocol

# design.md 1.5, spelled out rather than generated, so a wrong name in the module shows up here.
NAMES = (
    "out_count_1m", "out_sum_1m", "out_count_1h", "out_sum_1h", "out_count_24h", "out_sum_24h",
    "in_count_1m", "in_sum_1m", "in_count_1h", "in_sum_1h", "in_count_24h", "in_sum_24h",
    "dest_in_count_1m", "dest_in_sum_1m", "dest_in_count_1h", "dest_in_sum_1h",
    "dest_in_count_24h", "dest_in_sum_24h",
    "dest_out_count_1m", "dest_out_sum_1m", "dest_out_count_1h", "dest_out_sum_1h",
    "dest_out_count_24h", "dest_out_sum_24h",
    "attempt_count_1m", "attempt_sum_1m", "attempt_count_1h", "attempt_sum_1h",
    "attempt_count_24h", "attempt_sum_24h",
    "amount",
)  # fmt: skip

WIDTHS_MS = {"1m": 60 * 1000, "1h": 60 * 60 * 1000, "24h": 24 * 60 * 60 * 1000}


@dataclass(frozen=True)
class OracleLine:
    account: uuid.UUID
    amount: int


@dataclass(frozen=True)
class OracleEntry:
    id: uuid.UUID
    kind: str  # transfer, entry or reversal
    time_ms: int  # createdAt, truncated to the millisecond
    lines: tuple[OracleLine, ...]


@dataclass(frozen=True)
class OracleAttempt:
    request_id: str
    sender: uuid.UUID
    time_ms: int  # requestedAt, truncated to the millisecond
    amount: int


class ReplayedTransfer(Protocol):
    """What the oracle reads of a replayed PaySim transfer (ml/paysim.py's Transfer)."""

    @property
    def request_id(self) -> str: ...
    @property
    def sender(self) -> uuid.UUID: ...
    @property
    def receiver(self) -> uuid.UUID: ...
    @property
    def amount(self) -> int: ...
    @property
    def time_ms(self) -> int: ...


def replayed(
    transfers: Iterable[ReplayedTransfer],
) -> tuple[list[OracleEntry], list[OracleAttempt]]:
    """A replay's raw log (D40u): every transfer is a ledger entry at its time and an attempt by
    its sender at the same time."""
    entries, attempts = [], []
    for t in transfers:
        lines = (OracleLine(t.sender, -t.amount), OracleLine(t.receiver, t.amount))
        entries.append(OracleEntry(uuid.UUID(t.request_id), "transfer", t.time_ms, lines))
        attempts.append(OracleAttempt(t.request_id, t.sender, t.time_ms, t.amount))
    return entries, attempts


def net(entry: OracleEntry, account: uuid.UUID) -> int:
    total = 0
    for line in entry.lines:
        if line.account == account:
            total += line.amount
    return total


def features(
    entries: Iterable[OracleEntry],
    attempts: Iterable[OracleAttempt],
    sender: uuid.UUID,
    receiver: uuid.UUID,
    amount: int,
    t_ms: int,
) -> dict[str, int]:
    """The features of a decision at t: every entry and attempt with t - W <= time < t."""
    distinct_entries = list({entry.id: entry for entry in entries}.values())
    distinct_attempts = list({attempt.request_id: attempt for attempt in attempts}.values())
    result: dict[str, int] = {}
    for window, width in WIDTHS_MS.items():
        inside = [
            entry
            for entry in distinct_entries
            if entry.kind != "reversal" and t_ms - width <= entry.time_ms and entry.time_ms < t_ms
        ]
        families = {
            "out": [-net(e, sender) for e in inside if net(e, sender) < 0],
            "in": [net(e, sender) for e in inside if net(e, sender) > 0],
            "dest_in": [net(e, receiver) for e in inside if net(e, receiver) > 0],
            "dest_out": [-net(e, receiver) for e in inside if net(e, receiver) < 0],
            "attempt": [
                a.amount
                for a in distinct_attempts
                if a.sender == sender and t_ms - width <= a.time_ms and a.time_ms < t_ms
            ],
        }
        for family, values in families.items():
            result[f"{family}_count_{window}"] = len(values)
            result[f"{family}_sum_{window}"] = sum(values)
    result["amount"] = amount
    assert sorted(result) == sorted(NAMES)
    return {name: result[name] for name in NAMES}
