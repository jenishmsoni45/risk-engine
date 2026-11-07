"""D38's split by days (design.md section 8): the last 7 days test, the 2 days before them set the
threshold, and every earlier day trains.

A day is a UTC day of `requestedAt`, counted from PaySim's step 1 at 2026-01-01T00:00:00Z, so day
d holds steps 24d + 1 to 24d + 24 (D51.2). Phase 6 chooses the PaySim rule set's factor on the
training days (D54); Phase 7 trains and sets the threshold on them.
"""

from __future__ import annotations

from typing import Literal

import numpy as np
import numpy.typing as npt

from ml.paysim import STEP_1_MS
from risk.features.compute import DAY_MS

TEST_DAYS = 7
THRESHOLD_DAYS = 2

Part = Literal["train", "threshold", "test"]
PARTS: tuple[Part, ...] = ("train", "threshold", "test")


def day_of(time_ms: int) -> int:
    """The day of a transfer's time: 0 for 2026-01-01 UTC."""
    if time_ms < STEP_1_MS:
        raise ValueError(f"{time_ms} is before PaySim's step 1 (D51.2)")
    return (time_ms - STEP_1_MS) // DAY_MS


def part_of(day: int, last_day: int) -> Part:
    """Which set a day belongs to, when last_day is the data's last day."""
    if last_day < TEST_DAYS + THRESHOLD_DAYS:
        raise ValueError(
            f"D38 needs at least {TEST_DAYS + THRESHOLD_DAYS + 1} days so that one trains; "
            f"the data ends on day {last_day}"
        )
    if not 0 <= day <= last_day:
        raise ValueError(f"day {day} is outside days 0 to {last_day}")
    if day > last_day - TEST_DAYS:
        return "test"
    if day > last_day - TEST_DAYS - THRESHOLD_DAYS:
        return "threshold"
    return "train"


def masks(days: npt.NDArray[np.int64]) -> dict[Part, npt.NDArray[np.bool_]]:
    """Each set's transfers, given every transfer's day: one mask per part, by `part_of`, with
    the data's last day as the latest day given. Every transfer is in exactly one set."""
    if days.size == 0:
        raise ValueError("no transfers to split")
    last_day = int(days.max())
    by_part: dict[Part, list[int]] = {part: [] for part in PARTS}
    for day in np.unique(days).tolist():
        by_part[part_of(day, last_day)].append(day)
    return {part: np.isin(days, by_part[part]) for part in PARTS}
