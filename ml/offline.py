"""The offline feature pipeline (design.md section 8): the 31 features and the hour of day of every
transfer, from strictly earlier transfers, computed by the shared feature module.

Each loaded transfer is two things the online side also sees: a ledger event at its time, netted by
`net_entry` exactly as the consumer nets the stub's event, and an attempt by its sender at the same
time, as the decision API records it (D40u: in a replay every transfer is decided and posted). In
time order, `features_at` gives each transfer its features from the members before its time; the
transfers of one millisecond are all computed before any of them is added, so none sees another
(D40a, I6). Only members of the last 24 h are kept, so memory follows the window, not the file.
"""

from __future__ import annotations

import uuid
from collections import deque
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import timedelta

from ml.paysim import CURRENCY, Transfer
from risk.features.compute import (
    LONGEST_WINDOW_MS,
    DecisionMembers,
    Member,
    features_at,
    hour_of_day,
    net_entry,
)
from risk.features.events import LedgerEvent, Line
from risk.timestamps import EPOCH


@dataclass(frozen=True, slots=True)
class OfflineRow:
    transfer: Transfer
    features: dict[str, int]  # the 31 of design.md 1.5, in FEATURE_NAMES order
    hour: int  # D35's hour of day, from risk/features/


def ledger_event(transfer: Transfer) -> LedgerEvent:
    """The transfer as the event the stub publishes for it: the sender's line, then the
    receiver's (ledger_stub/transfers.py)."""
    moment = EPOCH + timedelta(milliseconds=transfer.time_ms)
    return LedgerEvent(
        id=uuid.UUID(transfer.request_id),
        kind="transfer",
        reverses_id=None,
        description=None,
        effective_at=moment,
        created_at=moment,
        decision_id=None,
        risk_check="decided",
        lines=(
            Line(1, transfer.sender, -transfer.amount, CURRENCY),
            Line(2, transfer.receiver, transfer.amount, CURRENCY),
        ),
    )


class _Members:
    """Each account's members of the last 24 h, oldest first, by family."""

    def __init__(self) -> None:
        self.sets: dict[tuple[uuid.UUID, str], deque[Member]] = {}
        # (added at, key) in time order: when each key may next have a member to drop.
        self.added: deque[tuple[int, tuple[uuid.UUID, str]]] = deque()

    def add(self, account: uuid.UUID, family: str, member: Member) -> None:
        key = (account, family)
        self.sets.setdefault(key, deque()).append(member)
        self.added.append((member.time_ms, key))

    def get(self, account: uuid.UUID, family: str) -> deque[Member] | tuple[()]:
        return self.sets.get((account, family), ())

    def drop_before(self, low_ms: int) -> None:
        """Drops every member older than low_ms, and every set left empty."""
        while self.added and self.added[0][0] < low_ms:
            _, key = self.added.popleft()
            members = self.sets.get(key)
            if members is None:
                continue
            while members and members[0].time_ms < low_ms:
                members.popleft()
            if not members:
                del self.sets[key]


def features(transfers: Iterable[Transfer]) -> Iterator[OfflineRow]:
    """Each transfer's features at its own time, from every earlier transfer given. The
    transfers must come in time order, as `ml.paysim.load` yields them."""
    members = _Members()
    pending: list[Transfer] = []  # the transfers at the current time, not yet added
    for transfer in transfers:
        t_ms = transfer.time_ms
        if pending and t_ms < pending[0].time_ms:
            raise ValueError(f"line {transfer.line}: transfers must come in time order")
        if pending and t_ms > pending[0].time_ms:
            for earlier in pending:
                _add(members, earlier)
            pending = []
        members.drop_before(t_ms - LONGEST_WINDOW_MS)
        sender, receiver = transfer.sender, transfer.receiver
        decision_members = DecisionMembers(
            sender_out=members.get(sender, "out"),
            sender_in=members.get(sender, "in"),
            sender_attempts=members.get(sender, "attempt"),
            receiver_in=members.get(receiver, "in"),
            receiver_out=members.get(receiver, "out"),
        )
        yield OfflineRow(
            transfer=transfer,
            features=features_at(t_ms, transfer.amount, decision_members),
            hour=hour_of_day(t_ms),
        )
        pending.append(transfer)


def _add(members: _Members, transfer: Transfer) -> None:
    for movement in net_entry(ledger_event(transfer)):
        members.add(
            movement.account_id, movement.direction, Member(transfer.time_ms, movement.amount)
        )
    members.add(transfer.sender, "attempt", Member(transfer.time_ms, transfer.amount))
