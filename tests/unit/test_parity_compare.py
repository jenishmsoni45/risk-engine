"""I7's comparison (ml/parity.py): exact equality between logged and offline snapshots, and a
report that names every difference and every missing row."""

from __future__ import annotations

import uuid

from ml.offline import OfflineRow
from ml.parity import compare
from ml.paysim import STEP_1_MS, Transfer
from risk.features.compute import FEATURE_NAMES


def row(**features: int) -> OfflineRow:
    transfer = Transfer(
        line=7,
        step=1,
        type="TRANSFER",
        request_id=str(uuid.uuid4()),
        sender=uuid.uuid4(),
        receiver=uuid.uuid4(),
        amount=500,
        time_ms=STEP_1_MS,
        is_fraud=False,
    )
    values = dict.fromkeys(FEATURE_NAMES, 0) | {"amount": 500} | features
    return OfflineRow(transfer=transfer, features=values, hour=0)


def test_equal_snapshots_pass_and_count_the_windows_that_held_members() -> None:
    rows = [row(), row(out_count_1m=2, out_sum_1m=30, dest_in_count_24h=1, dest_in_sum_24h=9)]
    report = compare(rows, {r.transfer.request_id: dict(r.features) for r in rows})
    assert report.ok
    assert (report.compared, report.equal) == (2, 2)
    assert report.nonzero == {"out_count_1m": 1, "dest_in_count_24h": 1}
    assert report.lines()[-1].startswith("I7 holds")


def test_a_different_value_is_named_with_both_sides() -> None:
    offline = row(in_sum_1h=100, in_count_1h=1)
    logged = dict(offline.features) | {"in_sum_1h": 99}
    report = compare([offline], {offline.transfer.request_id: logged})
    assert not report.ok
    (mismatch,) = report.mismatches
    assert mismatch.line == 7
    assert mismatch.differences == {"in_sum_1h": (99, 100)}
    assert "in_sum_1h online 99 offline 100" in "\n".join(report.lines())
    assert report.lines()[-1] == "I7 FAILS"


def test_a_missing_name_and_an_extra_name_are_differences() -> None:
    offline = row()
    logged = {k: v for k, v in offline.features.items() if k != "amount"} | {"surprise": 1}
    (mismatch,) = compare([offline], {offline.transfer.request_id: logged}).mismatches
    assert mismatch.differences == {"amount": (None, 500), "surprise": (1, 0)}


def test_a_replayed_transfer_without_a_decision_row_fails() -> None:
    offline = row()
    report = compare([offline], {})
    assert not report.ok
    assert report.missing == [offline.transfer.request_id]


def test_nothing_compared_is_not_a_pass() -> None:
    assert not compare([], {}).ok
