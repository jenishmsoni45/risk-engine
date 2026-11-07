"""The model card's metrics on the test days (design.md section 8).

- PR-AUC as average precision: over the distinct scores from the highest down, the precision at
  each score weighted by the recall gained there. Transfers sharing a score enter together, so the
  order of a tie never matters.
- At a threshold: recall (fraud held over all fraud), fraud value caught (the fraud amounts held
  over all fraud amounts), and the false-positive rate (non-fraud held over all non-fraud).

Pure numpy, so the tests run on the Mac, where LightGBM is never imported (D51.4).
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import numpy.typing as npt

from ml.threshold import Scores, held

Flags = npt.NDArray[np.bool_]
Cents = npt.NDArray[np.int64]


def _check(scores: Scores, fraud: Flags) -> None:
    if scores.shape != fraud.shape or scores.ndim != 1:
        raise ValueError("scores and fraud flags must be two arrays of one length")
    if not fraud.any():
        raise ValueError("no fraud among the transfers, so recall and PR-AUC are undefined")
    if fraud.all():
        raise ValueError("no transfer that isn't fraud, so the false-positive rate is undefined")


def average_precision(scores: Scores, fraud: Flags) -> float:
    """PR-AUC as average precision, with tied scores taken together."""
    _check(scores, fraud)
    order = np.argsort(-scores, kind="stable")
    ranked, hits = scores[order], fraud[order]
    true_pos = np.cumsum(hits)
    seen = np.arange(1, ranked.size + 1)
    # The last position of each run of equal scores: where the next score differs, and the end.
    last = np.append(np.flatnonzero(np.diff(ranked) != 0), ranked.size - 1)
    precision = true_pos[last] / seen[last]
    recall = true_pos[last] / true_pos[-1]
    gained = np.diff(recall, prepend=0.0)
    return float(np.sum(gained * precision))


@dataclass(frozen=True)
class AtThreshold:
    """What a threshold holds, counted; the card turns the counts into shares."""

    transfers: int
    held: int
    fraud: int
    fraud_held: int
    fraud_cents: int
    fraud_cents_held: int
    not_fraud: int
    not_fraud_held: int

    @property
    def recall(self) -> float:
        return self.fraud_held / self.fraud

    @property
    def value_caught(self) -> float:
        return self.fraud_cents_held / self.fraud_cents

    @property
    def false_positive_rate(self) -> float:
        return self.not_fraud_held / self.not_fraud


def at_threshold(scores: Scores, fraud: Flags, amounts: Cents, threshold: float) -> AtThreshold:
    """Counts of what the threshold holds among the transfers."""
    _check(scores, fraud)
    if amounts.shape != scores.shape:
        raise ValueError("amounts must have one value per transfer")
    hold = held(scores, threshold)
    return AtThreshold(
        transfers=int(scores.size),
        held=int(hold.sum()),
        fraud=int(fraud.sum()),
        fraud_held=int((hold & fraud).sum()),
        fraud_cents=int(amounts[fraud].sum()),
        fraud_cents_held=int(amounts[hold & fraud].sum()),
        not_fraud=int((~fraud).sum()),
        not_fraud_held=int((hold & ~fraud).sum()),
    )
