"""The PaySim loader (design.md section 8; D32, D33, D40p, D51.2, D52). Phase 6, criterion 2:
two runs on one CSV give identical output; UUID v5 mapping, cents rounded half to even, zero
amounts skipped and counted, and times spread evenly in file order within each hour.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from decimal import Decimal
from pathlib import Path

import pytest

from ml.generate import generate, write_csv
from ml.paysim import (
    NAMESPACE,
    STEP_1_MS,
    LoadStats,
    PaySimError,
    Transfer,
    cents,
    hour_times,
    load,
    request_id,
)
from risk.formats import IDEMPOTENCY_KEY

HOUR_MS = 3_600_000
# Pinned, so a changed namespace or start instant can't slip through: either would remap every
# account or move every time (D40p, D51.2).
PINNED_NAMESPACE = uuid.UUID("fa7c1ff5-c65e-5868-b6e0-cfa846804aaa")
PINNED_STEP_1_MS = 1_767_225_600_000  # 2026-01-01T00:00:00Z


def row(
    step: int, type_: str, amount: str, orig: str = "C1", dest: str = "C0", fraud: str = "0"
) -> tuple[str, ...]:
    return (str(step), type_, amount, orig, "0", "0", dest, "0", "0", fraud, "0")


def write(tmp_path: Path, rows: Sequence[tuple[str, ...]]) -> Path:
    path = tmp_path / "paysim.csv"
    write_csv(path, rows)
    return path


def loaded(tmp_path: Path, rows: Sequence[tuple[str, ...]]) -> tuple[list[Transfer], LoadStats]:
    stats = LoadStats()
    return list(load(write(tmp_path, rows), stats=stats)), stats


def test_two_runs_on_one_csv_give_identical_output(tmp_path: Path) -> None:
    path = write(tmp_path, generate(seed=3))
    first_stats, second_stats = LoadStats(), LoadStats()
    first = list(load(path, stats=first_stats))
    second = list(load(path, stats=second_stats))
    assert first == second
    assert first_stats == second_stats
    assert len(first) > 100


def test_names_become_uuid_v5_in_one_fixed_namespace(tmp_path: Path) -> None:
    assert NAMESPACE == PINNED_NAMESPACE
    transfers, _ = loaded(
        tmp_path,
        [row(1, "TRANSFER", "10.00", "C111", "C222"), row(1, "CASH_OUT", "5.00", "C222", "C333")],
    )
    first, second = transfers
    assert first.sender == uuid.uuid5(PINNED_NAMESPACE, "C111")
    assert first.receiver == uuid.uuid5(PINNED_NAMESPACE, "C222")
    assert first.sender.version == 5
    # A name is one account, whichever side it is on.
    assert second.sender == first.receiver


def test_each_key_is_the_uuid_v5_of_its_step_and_line(tmp_path: Path) -> None:
    transfers, _ = loaded(tmp_path, [row(1, "PAYMENT", "1.00"), row(2, "TRANSFER", "1.00")])
    (transfer,) = transfers
    assert transfer.line == 3  # the header is line 1
    assert transfer.request_id == str(uuid.uuid5(PINNED_NAMESPACE, "paysim:2:3"))
    assert transfer.request_id == request_id(2, 3)
    assert IDEMPOTENCY_KEY.fullmatch(transfer.request_id)


@pytest.mark.parametrize(
    ("amount", "expected"),
    [
        ("181.0", 18100),
        ("9839.64", 983964),
        ("0.125", 12),  # half to even: 12.5 cents to 12
        ("0.135", 14),  # and 13.5 to 14
        ("2.675", 268),
        ("1.005", 100),
        ("0.006", 1),
        ("92445516.64", 9244551664),
    ],
)
def test_amounts_become_cents_rounded_half_to_even(amount: str, expected: int) -> None:
    assert cents(Decimal(amount)) == expected


def test_the_loader_rounds_each_amount_half_to_even(tmp_path: Path) -> None:
    transfers, _ = loaded(tmp_path, [row(1, "TRANSFER", "100.125"), row(1, "CASH_OUT", "0.135")])
    assert [t.amount for t in transfers] == [10012, 14]


def test_zero_amounts_are_skipped_and_counted(tmp_path: Path) -> None:
    transfers, stats = loaded(
        tmp_path,
        [
            row(1, "TRANSFER", "0.0", fraud="1"),
            row(1, "CASH_OUT", "0.004"),
            row(1, "CASH_OUT", "0.005"),  # half to even: 0.5 cents to 0
            row(1, "PAYMENT", "0.0"),  # not a loaded type: not counted as a skipped transfer
            row(1, "TRANSFER", "0.01"),
        ],
    )
    assert [t.amount for t in transfers] == [1]
    assert stats.zero_amount == 3
    assert stats.rows == 5
    assert stats.transfers == 1


def test_only_transfer_and_cash_out_rows_are_loaded(tmp_path: Path) -> None:
    types = ("CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER")
    transfers, stats = loaded(tmp_path, [row(1, t, "1.00") for t in types])
    assert [t.type for t in transfers] == ["CASH_OUT", "TRANSFER"]
    assert dict(stats.by_type) == dict.fromkeys(types, 1)


def test_times_spread_evenly_in_file_order_within_each_hour(tmp_path: Path) -> None:
    rows = [
        row(1, "TRANSFER", "1.00", "C1"),
        row(1, "PAYMENT", "1.00", "C9"),  # not loaded, so it takes no slot
        row(1, "CASH_OUT", "2.00", "C2"),
        row(1, "TRANSFER", "3.00", "C3"),
        row(3, "TRANSFER", "4.00", "C4"),
    ]
    transfers, _ = loaded(tmp_path, rows)
    assert STEP_1_MS == PINNED_STEP_1_MS
    assert [(t.amount, t.time_ms) for t in transfers] == [
        (100, PINNED_STEP_1_MS),
        (200, PINNED_STEP_1_MS + HOUR_MS // 3),
        (300, PINNED_STEP_1_MS + 2 * HOUR_MS // 3),
        (400, PINNED_STEP_1_MS + 2 * HOUR_MS),  # step 3 starts two hours after step 1
    ]


def test_an_hours_times_are_whole_milliseconds_rounded_down_and_strictly_increasing() -> None:
    times = hour_times(2, 7)
    start = PINNED_STEP_1_MS + HOUR_MS
    assert times == [start + i * HOUR_MS // 7 for i in range(7)]
    assert times[1] - times[0] == 514_285  # 3,600,000 / 7, rounded down
    busy = hour_times(5, HOUR_MS)  # one transfer per millisecond still increases
    assert all(b - a == 1 for a, b in zip(busy, busy[1:], strict=False))


def test_an_hour_with_more_transfers_than_milliseconds_is_refused() -> None:
    with pytest.raises(PaySimError, match="more than its hour's milliseconds"):
        hour_times(1, HOUR_MS + 1)


def test_steps_that_go_backwards_are_refused(tmp_path: Path) -> None:
    rows = [row(2, "TRANSFER", "1.00"), row(1, "TRANSFER", "1.00")]
    with pytest.raises(PaySimError, match="line 3: step 1 comes after step 2"):
        loaded(tmp_path, rows)


def test_a_transfer_to_the_same_account_is_refused(tmp_path: Path) -> None:
    with pytest.raises(PaySimError, match="line 2: C7 sends to itself"):
        loaded(tmp_path, [row(1, "CASH_OUT", "1.00", "C7", "C7")])


def test_a_file_that_isnt_paysims_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "other.csv"
    path.write_text("a,b,c\n1,2,3\n")
    with pytest.raises(PaySimError, match="header"):
        list(load(path))


@pytest.mark.parametrize(
    ("values", "message"),
    [
        (("0", "TRANSFER", "1.00", "C1", "0", "0", "C2", "0", "0", "0", "0"), "step"),
        (("x", "TRANSFER", "1.00", "C1", "0", "0", "C2", "0", "0", "0", "0"), "step"),
        (("1", "REFUND", "1.00", "C1", "0", "0", "C2", "0", "0", "0", "0"), "unknown type"),
        (("1", "TRANSFER", "ten", "C1", "0", "0", "C2", "0", "0", "0", "0"), "amount"),
        (("1", "TRANSFER", "-1", "C1", "0", "0", "C2", "0", "0", "0", "0"), "amount"),
        (("1", "TRANSFER", "NaN", "C1", "0", "0", "C2", "0", "0", "0", "0"), "amount"),
        (("1", "TRANSFER", "1.00", "", "0", "0", "C2", "0", "0", "0", "0"), "nameOrig"),
        (("1", "TRANSFER", "1.00", "C1", "0", "0", "C2", "0", "0", "2", "0"), "isFraud"),
        (("1", "TRANSFER", "1.00", "C1", "0", "0", "C2", "0", "0", "0", "x"), "isFlaggedFraud"),
        (("1", "TRANSFER", "1.00", "C1"), "columns"),
    ],
)
def test_a_malformed_row_is_refused_with_its_line(
    tmp_path: Path, values: tuple[str, ...], message: str
) -> None:
    with pytest.raises(PaySimError, match=rf"line 2: .*{message}"):
        loaded(tmp_path, [values])


def test_the_generator_is_seeded() -> None:
    assert generate(seed=5) == generate(seed=5)
    assert generate(seed=5) != generate(seed=6)


def test_generated_rows_hold_every_type_and_paysims_fraud_pattern(tmp_path: Path) -> None:
    rows = generate(seed=1)
    assert {r[1] for r in rows} == {"CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"}
    # A fraud TRANSFER to a mule followed by the mule's fraud CASH_OUT of the same amount.
    pairs = [
        (a, b)
        for a, b in zip(rows, rows[1:], strict=False)
        if a[1] == "TRANSFER" and b[1] == "CASH_OUT" and a[9] == b[9] == "1"
    ]
    assert pairs
    assert all(a[6] == b[3] and a[2] == b[2] for a, b in pairs)
    transfers, stats = loaded(tmp_path, rows)
    assert stats.zero_amount > 0
    assert sum(t.is_fraud for t in transfers) >= 2 * len(pairs)
