"""The offline pipeline (design.md section 8) against the brute-force oracle (tests/oracle.py),
which shares no code with risk/features/: every transfer's features at its own time, from the
transfers before it as ledger events and as attempts, and the hour of day (D35, D51.2).
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest

from ml.generate import generate, write_csv
from ml.offline import features, ledger_event
from ml.paysim import STEP_1_MS, Transfer, load
from risk.features.events import parse_event
from tests import oracle
from tests.oracle import OracleAttempt, OracleEntry, OracleLine

A, B, C = (uuid.UUID(f"0ff1ce00-0000-4000-8000-00000000000{n}") for n in (1, 2, 3))


def transfer(sender: uuid.UUID, receiver: uuid.UUID, amount: int, time_ms: int) -> Transfer:
    return Transfer(
        line=0,
        step=1,
        type="TRANSFER",
        request_id=str(uuid.uuid4()),
        sender=sender,
        receiver=receiver,
        amount=amount,
        time_ms=time_ms,
        is_fraud=False,
    )


def expected(log: list[Transfer], index: int) -> dict[str, int]:
    """The oracle's features for log[index], from every transfer of the log before its time."""
    target = log[index]
    entries = [
        OracleEntry(
            id=uuid.UUID(t.request_id),
            kind="transfer",
            time_ms=t.time_ms,
            lines=(OracleLine(t.sender, -t.amount), OracleLine(t.receiver, t.amount)),
        )
        for t in log
    ]
    attempts = [OracleAttempt(t.request_id, t.sender, t.time_ms, t.amount) for t in log]
    return oracle.features(
        entries, attempts, target.sender, target.receiver, target.amount, target.time_ms
    )


def test_generated_transfers_get_the_oracles_features_and_their_hour(tmp_path: Path) -> None:
    path = tmp_path / "generated.csv"
    write_csv(path, generate(seed=2))
    log = list(load(path))
    rows = list(features(log))
    assert [r.transfer for r in rows] == log
    for index, row in enumerate(rows):
        assert row.features == expected(log, index), row.transfer
        assert row.hour == (row.transfer.time_ms - STEP_1_MS) // 3_600_000 % 24
    # Not a comparison of empty windows: some transfers see a minute's worth of history.
    assert any(r.features["dest_in_count_1m"] for r in rows)
    assert any(r.features["out_count_1m"] for r in rows)


def test_transfers_in_one_millisecond_dont_see_each_other() -> None:
    t = STEP_1_MS + 5_000
    log = [transfer(A, B, 100, t - 1), transfer(A, B, 200, t), transfer(A, C, 300, t)]
    rows = list(features(log))
    for row in rows[1:]:
        assert row.features["out_count_1m"] == 1  # only the one before t
        assert row.features["attempt_sum_1m"] == 100
    assert rows[2].features["dest_in_count_24h"] == 0  # C's inflow at t is its own


def test_window_edges_follow_the_shared_module() -> None:
    t = STEP_1_MS + 2 * 86_400_000
    log = [
        transfer(A, B, 1, t - 86_400_000),  # exactly 24 h before t: inside the 24 h window
        transfer(A, B, 2, t - 3_600_000),  # exactly 1 h before: inside the 1 h window
        transfer(A, B, 4, t - 60_000),  # exactly 1 min before: inside the 1 min window
        transfer(B, A, 8, t),  # B pays A back at t
    ]
    last = list(features(log))[-1].features
    # B is the sender now, so its inflows are `in_*`; A, the receiver, sent all three.
    for family in ("in", "dest_out"):
        assert (last[f"{family}_count_1m"], last[f"{family}_sum_1m"]) == (1, 4)
        assert (last[f"{family}_count_1h"], last[f"{family}_sum_1h"]) == (2, 6)
        assert (last[f"{family}_count_24h"], last[f"{family}_sum_24h"]) == (3, 7)
    assert last == expected(log, 3)


def test_members_older_than_a_day_are_forgotten_without_changing_any_feature() -> None:
    day = 86_400_000
    log = [transfer(A, B, 10 + n, STEP_1_MS + n * day // 3) for n in range(12)]
    for index, row in enumerate(features(log)):
        assert row.features == expected(log, index)


def test_transfers_out_of_time_order_are_refused() -> None:
    log = [transfer(A, B, 1, STEP_1_MS + 10), transfer(A, B, 1, STEP_1_MS + 9)]
    with pytest.raises(ValueError, match="time order"):
        list(features(log))


def test_a_transfers_ledger_event_parses_like_the_stubs() -> None:
    """The event the offline side nets is the stub's event for the transfer (design.md 1.2)."""
    t = transfer(A, B, 1234, STEP_1_MS + 1)
    event = ledger_event(t)
    assert event.kind == "transfer"
    assert [(line.account_id, line.amount) for line in event.lines] == [(A, -1234), (B, 1234)]
    stub_event = {
        "id": t.request_id,
        "kind": "transfer",
        "reversesId": None,
        "description": None,
        "effectiveAt": "2026-01-01T00:00:00.001Z",
        "createdAt": "2026-01-01T00:00:00.001Z",
        "decisionId": None,
        "riskCheck": "decided",
        "lines": [
            {"id": 1, "accountId": str(A), "amount": -1234, "currency": "CAD"},
            {"id": 2, "accountId": str(B), "amount": 1234, "currency": "CAD"},
        ],
    }
    assert parse_event(t.request_id.encode(), json.dumps(stub_event).encode()) == event
