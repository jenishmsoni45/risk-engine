"""The model card's metrics (ml/metrics.py, design.md section 8): PR-AUC as average precision, and
recall, fraud value caught and false-positive rate at a threshold."""

from __future__ import annotations

import numpy as np
import pytest
from hypothesis import given
from hypothesis import strategies as st

from ml.metrics import at_threshold, average_precision


def arrays(scores: list[float], fraud: list[bool]) -> tuple[np.ndarray, np.ndarray]:
    return np.array(scores, dtype=np.float64), np.array(fraud, dtype=np.bool_)


def test_a_perfect_ranking_has_an_average_precision_of_1() -> None:
    assert average_precision(*arrays([0.9, 0.8, 0.2, 0.1], [True, True, False, False])) == 1.0


def test_average_precision_by_hand() -> None:
    # Ranked: fraud, other, fraud, other. Precision at each fraud: 1/1, then 2/3.
    s, f = arrays([0.9, 0.8, 0.7, 0.1], [True, False, True, False])
    assert average_precision(s, f) == pytest.approx((1 + 2 / 3) / 2)


def test_tied_scores_enter_together_whatever_their_order() -> None:
    # The two at 0.5 count as one step: recall 1 at precision 1/2.
    one = average_precision(*arrays([0.5, 0.5, 0.1], [True, False, False]))
    other = average_precision(*arrays([0.5, 0.5, 0.1], [False, True, False]))
    assert one == other == pytest.approx(0.5)


@given(
    st.lists(
        st.tuples(st.floats(0, 1, allow_nan=False), st.booleans()), min_size=2, max_size=300
    ).filter(lambda rows: any(f for _, f in rows) and not all(f for _, f in rows))
)
def test_average_precision_is_above_0_at_most_1_and_1_for_a_perfect_ranking(
    rows: list[tuple[float, bool]],
) -> None:
    s, f = arrays([v for v, _ in rows], [x for _, x in rows])
    value = average_precision(s, f)
    assert 0 < value <= 1
    # Scoring fraud above everything else is a perfect ranking.
    assert average_precision(f.astype(np.float64), f) == pytest.approx(1.0)


def test_counts_at_a_threshold() -> None:
    s, f = arrays([0.9, 0.8, 0.6, 0.3, 0.2], [True, False, True, True, False])
    amounts = np.array([500, 40, 300, 200, 10], dtype=np.int64)
    result = at_threshold(s, f, amounts, 0.6)
    assert (result.transfers, result.held) == (5, 3)
    assert (result.fraud, result.fraud_held) == (3, 2)
    assert (result.fraud_cents, result.fraud_cents_held) == (1000, 800)
    assert (result.not_fraud, result.not_fraud_held) == (2, 1)
    assert result.recall == pytest.approx(2 / 3)
    assert result.value_caught == pytest.approx(0.8)
    assert result.false_positive_rate == pytest.approx(0.5)


def test_metrics_refuse_data_without_both_kinds_of_transfer() -> None:
    with pytest.raises(ValueError, match="no fraud"):
        average_precision(*arrays([0.1, 0.2], [False, False]))
    with pytest.raises(ValueError, match="no transfer that isn't fraud"):
        at_threshold(*arrays([0.1, 0.2], [True, True]), np.array([1, 2]), 0.5)


def test_metrics_refuse_mismatched_arrays() -> None:
    with pytest.raises(ValueError, match="one length"):
        average_precision(np.array([0.1, 0.2]), np.array([True]))
    with pytest.raises(ValueError, match="one value per transfer"):
        at_threshold(*arrays([0.1, 0.2], [True, False]), np.array([1]), 0.5)
