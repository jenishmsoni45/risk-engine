"""The threshold holds at most 1% of the threshold days' transfers, and within one transfer of 1%
unless several transfers share the score at the threshold (Phase 7, criterion 4; D34, D55.3)."""

from __future__ import annotations

import math

import numpy as np
import pytest
from hypothesis import given
from hypothesis import strategies as st

from ml.threshold import capacity, held, threshold_for

scores = st.floats(min_value=0, max_value=1, allow_nan=False, allow_infinity=False)


def test_capacity_is_one_transfer_in_a_hundred_rounded_down() -> None:
    assert [capacity(n) for n in (0, 99, 100, 199, 200, 18_345)] == [0, 0, 1, 1, 2, 183]
    with pytest.raises(ValueError, match="negative"):
        capacity(-1)


@given(st.lists(scores, unique=True, min_size=0, max_size=2_000))
def test_distinct_scores_hold_exactly_the_capacity_within_one_transfer_of_1_percent(
    values: list[float],
) -> None:
    s = np.array(values, dtype=np.float64)
    count = int(held(s, threshold_for(s, capacity(s.size))).sum())
    assert count == capacity(s.size)
    assert 0 <= s.size / 100 - count < 1


# Few distinct values, so ties are common, at the line and elsewhere.
tied = st.lists(st.sampled_from([0.0, 0.1, 0.25, 0.5, 0.75, 0.9, 1.0]), max_size=2_000)


@given(tied)
def test_ties_never_push_the_held_count_above_the_capacity(values: list[float]) -> None:
    s = np.array(values, dtype=np.float64)
    limit = capacity(s.size)
    threshold = threshold_for(s, limit)
    assert int(held(s, threshold).sum()) <= limit


@given(tied)
def test_the_threshold_is_the_lowest_score_that_keeps_within_the_capacity(
    values: list[float],
) -> None:
    s = np.array(values, dtype=np.float64)
    limit = capacity(s.size)
    threshold = threshold_for(s, limit)
    lower = s[s < threshold]
    if lower.size:
        assert int(held(s, float(lower.max())).sum()) > limit


def test_a_tie_across_the_line_holds_fewer_rather_than_more() -> None:
    # 400 transfers may hold 4: two at 0.9, then three tied at 0.8 would make 5.
    s = np.array([0.9, 0.9, 0.8, 0.8, 0.8] + [0.1] * 395, dtype=np.float64)
    threshold = threshold_for(s, capacity(s.size))
    assert threshold == 0.9
    assert int(held(s, threshold).sum()) == 2


def test_a_top_score_shared_by_more_than_the_capacity_holds_none() -> None:
    s = np.full(500, 0.3, dtype=np.float64)
    assert threshold_for(s, capacity(s.size)) == math.inf
    assert not held(s, math.inf).any()


def test_fewer_than_100_transfers_hold_none() -> None:
    s = np.linspace(0, 1, 99)
    assert threshold_for(s, capacity(s.size)) == math.inf


def test_held_means_at_or_above_the_threshold() -> None:
    s = np.array([0.2, 0.5, 0.7], dtype=np.float64)
    assert held(s, 0.5).tolist() == [False, True, True]


@pytest.mark.parametrize("bad", [math.nan, math.inf, -math.inf])
def test_a_score_that_isnt_a_finite_number_is_refused(bad: float) -> None:
    with pytest.raises(ValueError, match="finite"):
        threshold_for(np.array([0.1, bad]), 1)


def test_a_negative_capacity_is_refused() -> None:
    with pytest.raises(ValueError, match="negative"):
        threshold_for(np.array([0.1]), -1)
