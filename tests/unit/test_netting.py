"""Netting an entry per account (design.md 3.2, D17). Phase 3, criterion 3."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from risk.features.compute import DecisionMembers, Member, Movement, features_at, net_entry
from risk.features.events import LedgerEvent, Line

A = uuid.UUID("00000000-0000-4000-8000-00000000000a")
B = uuid.UUID("00000000-0000-4000-8000-00000000000b")
C = uuid.UUID("00000000-0000-4000-8000-00000000000c")
AT = datetime(2026, 11, 20, 15, 4, 5, 123000, tzinfo=UTC)


def event(kind: str, *lines: tuple[uuid.UUID, int]) -> LedgerEvent:
    return LedgerEvent(
        id=uuid.uuid4(),
        kind=kind,
        reverses_id=uuid.uuid4() if kind == "reversal" else None,
        description=None,
        effective_at=AT,
        created_at=AT,
        decision_id=None,
        risk_check="none",
        lines=tuple(
            Line(id=n, account_id=account, amount=amount, currency="CAD")
            for n, (account, amount) in enumerate(lines, start=1)
        ),
    )


def test_minus_100_and_plus_30_on_one_account_is_one_outflow_of_70() -> None:
    movements = net_entry(event("entry", (A, -100), (B, 100), (C, -30), (A, 30)))
    assert movements == [
        Movement(A, "out", 70),
        Movement(B, "in", 100),
        Movement(C, "out", 30),
    ]


def test_the_outflow_of_70_counts_once_with_its_net() -> None:
    (out,) = [
        m for m in net_entry(event("entry", (A, -100), (B, 70), (A, 30))) if m.account_id == A
    ]
    t = 1_000_000
    members = DecisionMembers(
        sender_out=[Member(time_ms=t - 1, amount=out.amount)],
        sender_in=[],
        sender_attempts=[],
        receiver_in=[],
        receiver_out=[],
    )
    features = features_at(t, 5, members)
    assert (features["out_count_1m"], features["out_sum_1m"]) == (1, 70)


def test_a_zero_net_adds_nothing() -> None:
    assert net_entry(event("entry", (A, -50), (B, 50), (A, 50), (B, -50))) == []
    assert net_entry(event("entry", (A, -50), (A, 50), (B, -7), (C, 7))) == [
        Movement(B, "out", 7),
        Movement(C, "in", 7),
    ]


def test_a_reversal_adds_nothing() -> None:
    assert net_entry(event("reversal", (A, 100), (B, -100))) == []


def test_a_transfer_is_an_outflow_for_the_sender_and_an_inflow_for_the_receiver() -> None:
    assert net_entry(event("transfer", (A, -12500), (B, 12500))) == [
        Movement(A, "out", 12500),
        Movement(B, "in", 12500),
    ]


def test_the_order_of_the_lines_does_not_matter() -> None:
    lines = [(A, -100), (B, 70), (C, 30), (A, 40), (B, -40)]
    assert net_entry(event("entry", *lines)) == net_entry(event("entry", *reversed(lines)))


def test_amounts_beyond_64_bits_net_exactly() -> None:
    big = 2**63 - 1
    assert net_entry(event("entry", (A, -big), (A, -big), (B, big), (B, big))) == [
        Movement(A, "out", 2 * big),
        Movement(B, "in", 2 * big),
    ]
