"""The only feature logic (design.md 3.5): netting an entry, window membership, the 31 features
from members and t, and their names.

The consumer, the decision API and (2b) the offline pipeline all call this module; the
brute-force oracle in tests/oracle.py checks it. Everything here is pure: no I/O and no clock.
Times are epoch milliseconds, already truncated to the millisecond (design.md 3.1).
"""

from __future__ import annotations

import uuid
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

from risk.features.events import LedgerEvent

MINUTE_MS = 60_000
HOUR_MS = 60 * MINUTE_MS
DAY_MS = 24 * HOUR_MS

# D19's windows, shortest first: the name suffix and the width.
WINDOWS: tuple[tuple[str, int], ...] = (("1m", MINUTE_MS), ("1h", HOUR_MS), ("24h", DAY_MS))
LONGEST_WINDOW_MS = DAY_MS

# The member families a decision reads, in design.md 1.5's order (D40b).
FAMILIES = ("out", "in", "dest_in", "dest_out", "attempt")

FEATURE_NAMES: tuple[str, ...] = (
    *(
        f"{family}_{measure}_{window}"
        for family in FAMILIES
        for window, _ in WINDOWS
        for measure in ("count", "sum")
    ),
    "amount",
)

Direction = Literal["out", "in"]


@dataclass(frozen=True)
class Movement:
    """One account's net change in one entry, which becomes one window member (design.md 3.2)."""

    account_id: uuid.UUID
    direction: Direction
    amount: int  # the absolute net, at least 1


def net_entry(event: LedgerEvent) -> list[Movement]:
    """The entry's net change per account: a negative net is an outflow, a positive one an inflow,
    and zero adds nothing. A reversal adds nothing (D17). Sorted by account, so the result
    doesn't depend on the order of the lines."""
    if event.kind == "reversal":
        return []
    nets: defaultdict[uuid.UUID, int] = defaultdict(int)
    for line in event.lines:
        nets[line.account_id] += line.amount
    return [
        Movement(account_id, "out" if net < 0 else "in", abs(net))
        for account_id, net in sorted(nets.items())
        if net != 0
    ]


@dataclass(frozen=True)
class Member:
    """A window member: an entry's net for one account, or an attempt."""

    time_ms: int  # createdAt for ledger events, requestedAt for attempts
    amount: int


@dataclass(frozen=True)
class DecisionMembers:
    """The five sets a decision reads (design.md 3.4). Members outside [t - 24 h, t) may be
    present; the windows ignore them."""

    sender_out: Sequence[Member]
    sender_in: Sequence[Member]
    sender_attempts: Sequence[Member]
    receiver_in: Sequence[Member]
    receiver_out: Sequence[Member]


def in_window(time_ms: int, t_ms: int, width_ms: int) -> bool:
    """D40a: a member counts in a window of width W at decision time t when t - W ≤ time < t."""
    return t_ms - width_ms <= time_ms < t_ms


def read_range(t_ms: int) -> tuple[int, int]:
    """The scores a decision at t needs, as [low, high): every window lies inside it."""
    return t_ms - LONGEST_WINDOW_MS, t_ms


def features_at(t_ms: int, amount: int, members: DecisionMembers) -> dict[str, int]:
    """The 31 features of design.md 1.5 for a decision at t, in FEATURE_NAMES order."""
    by_family: dict[str, Sequence[Member]] = {
        "out": members.sender_out,
        "in": members.sender_in,
        "dest_in": members.receiver_in,
        "dest_out": members.receiver_out,
        "attempt": members.sender_attempts,
    }
    values: dict[str, int] = {}
    for family in FAMILIES:
        for window, width_ms in WINDOWS:
            amounts = [m.amount for m in by_family[family] if in_window(m.time_ms, t_ms, width_ms)]
            values[f"{family}_count_{window}"] = len(amounts)
            values[f"{family}_sum_{window}"] = sum(amounts)
    values["amount"] = amount
    return {name: values[name] for name in FEATURE_NAMES}
