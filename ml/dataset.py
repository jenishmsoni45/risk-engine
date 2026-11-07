"""The offline features as the model's arrays (design.md section 8; D35, D54, D55.2).

Each loaded transfer becomes one row of the 31 features of D19, in `FEATURE_NAMES` order, computed
by the offline pipeline on the shared feature module (CLAUDE.md), plus its fraud label, amount and
day (D38). The hour of day is not an input (D54).

The features are whole numbers, held as float64, which is exact up to 2^53, far above any count or
sum of cents here. LightGBM would turn any other dtype into float32, which can't hold every amount
in cents exactly, so the shadow scorer must pass float64 too.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

import numpy as np
import numpy.typing as npt

from ml.offline import features
from ml.paysim import Transfer
from ml.split import day_of
from risk.features.compute import FEATURE_NAMES

# The model's inputs, in column order: D35's inputs without the hour of day (D54).
INPUTS: tuple[str, ...] = FEATURE_NAMES
CHUNK = 65_536  # rows gathered as Python values before each becomes an array


@dataclass(frozen=True)
class Arrays:
    x: npt.NDArray[np.float64]  # one row per transfer, the INPUTS in order
    fraud: npt.NDArray[np.bool_]
    amount: npt.NDArray[np.int64]  # cents
    day: npt.NDArray[np.int64]  # D38's day of the transfer's time

    def __len__(self) -> int:
        return int(self.fraud.size)

    def rows(self, mask: npt.NDArray[np.bool_]) -> Arrays:
        return Arrays(self.x[mask], self.fraud[mask], self.amount[mask], self.day[mask])


def arrays(transfers: Iterable[Transfer]) -> Arrays:
    """The transfers' features and labels, in the order given, which must be time order."""
    blocks: list[npt.NDArray[np.float64]] = []
    rows: list[list[int]] = []
    fraud: list[bool] = []
    amount: list[int] = []
    day: list[int] = []
    for row in features(transfers):
        rows.append([row.features[name] for name in INPUTS])
        fraud.append(row.transfer.is_fraud)
        amount.append(row.transfer.amount)
        day.append(day_of(row.transfer.time_ms))
        if len(rows) == CHUNK:
            blocks.append(np.array(rows, dtype=np.float64))
            rows = []
    if rows or not blocks:
        blocks.append(np.array(rows, dtype=np.float64).reshape(len(rows), len(INPUTS)))
    return Arrays(
        x=np.concatenate(blocks),
        fraud=np.array(fraud, dtype=np.bool_),
        amount=np.array(amount, dtype=np.int64),
        day=np.array(day, dtype=np.int64),
    )
