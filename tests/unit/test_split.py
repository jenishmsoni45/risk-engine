"""D38's split by days (ml/split.py): day d holds PaySim steps 24d + 1 to 24d + 24 from UTC
midnight (D51.2); the last 7 days test, the 2 days before them set the threshold, the rest train.
"""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from ml.paysim import step_start_ms
from ml.split import day_of, part_of
from risk.features.compute import HOUR_MS


def test_a_day_holds_24_steps_from_midnight() -> None:
    assert day_of(step_start_ms(1)) == 0
    assert day_of(step_start_ms(24) + HOUR_MS - 1) == 0
    assert day_of(step_start_ms(25)) == 1
    assert day_of(step_start_ms(743)) == 30


@given(st.integers(min_value=1, max_value=743), st.integers(min_value=0, max_value=HOUR_MS - 1))
def test_every_time_in_a_step_falls_on_that_steps_day(step: int, offset: int) -> None:
    assert day_of(step_start_ms(step) + offset) == (step - 1) // 24


def test_a_time_before_step_1_is_refused() -> None:
    with pytest.raises(ValueError, match="before PaySim's step 1"):
        day_of(step_start_ms(1) - 1)


def test_the_last_7_days_test_and_the_2_before_them_set_the_threshold() -> None:
    parts = [part_of(day, 30) for day in range(31)]
    assert parts == ["train"] * 22 + ["threshold"] * 2 + ["test"] * 7


def test_data_too_short_to_leave_a_training_day_is_refused() -> None:
    assert part_of(0, 9) == "train"
    with pytest.raises(ValueError, match="at least 10 days"):
        part_of(0, 8)


@pytest.mark.parametrize("day", [-1, 31])
def test_a_day_outside_the_data_is_refused(day: int) -> None:
    with pytest.raises(ValueError, match="outside days 0 to 30"):
        part_of(day, 30)
