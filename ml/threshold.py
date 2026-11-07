"""The model's threshold, set by review capacity (design.md section 8; D34, D55.3).

Of n transfers, `capacity(n)` = ⌊n / 100⌋ may be held. The threshold is the lowest score at which
at most that many transfers score at or above it, and a transfer is held when its score is at or
above the threshold. When no tie straddles the line, exactly the capacity is held, within one
transfer of 1%; when transfers share the score at the line, all of them stay below it, so fewer
are held and never more. When even the top score is shared by more transfers than may be held,
the threshold is infinite and holds none.

Pure numpy, so the tests run on the Mac, where LightGBM is never imported (D51.4).
"""

from __future__ import annotations

import math

import numpy as np
import numpy.typing as npt

HOLD_SHARE = 100  # one transfer in 100 held (D34)

Scores = npt.NDArray[np.float64]


def capacity(transfers: int) -> int:
    """How many of `transfers` the review capacity may hold: ⌊1%⌋ (D34)."""
    if transfers < 0:
        raise ValueError(f"a count of transfers can't be negative: {transfers}")
    return transfers // HOLD_SHARE


def threshold_for(scores: Scores, at_most: int) -> float:
    """The lowest score at which at most `at_most` of `scores` are at or above it (D55.3)."""
    if at_most < 0:
        raise ValueError(f"at_most can't be negative: {at_most}")
    if not np.all(np.isfinite(scores)):
        raise ValueError("every score must be a finite number")
    distinct, counts = np.unique(scores, return_counts=True)  # ascending
    at_or_above = np.cumsum(counts[::-1])[::-1]  # at_or_above[i]: scores >= distinct[i]
    allowed = np.flatnonzero(at_or_above <= at_most)
    if allowed.size == 0:
        return math.inf
    return float(distinct[allowed[0]])


def held(scores: Scores, threshold: float) -> npt.NDArray[np.bool_]:
    """Which transfers the threshold holds: those scoring at or above it."""
    result: npt.NDArray[np.bool_] = scores >= threshold
    return result
