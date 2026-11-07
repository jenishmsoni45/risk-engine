"""A stand-in for LightGBM, so the training script's tests run on the Mac, where LightGBM is never
imported (D51.4), and generated PaySim-format data for them (D37)."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import numpy.typing as npt

from ml.generate import generate, write_csv

# 12 days: the split leaves 3 training days, 2 threshold days and 7 test days (D38).
STEPS = 12 * 24


def generated_csv(folder: Path, seed: int = 1, steps: int = STEPS) -> Path:
    path = folder / f"generated-{seed}-{steps}.csv"
    write_csv(path, generate(seed, steps))
    return path


class FakeModel:
    """Scores by one input column, scaled to [0, 1); its text names the inputs as LightGBM does."""

    def __init__(self, names: Sequence[str], column: str) -> None:
        self.names = tuple(names)
        self.index = self.names.index(column)

    def predict(self, data: npt.NDArray[np.float64], /) -> npt.NDArray[np.float64]:
        values = data[:, self.index]
        result: npt.NDArray[np.float64] = values / (values.max() + 1) if values.size else values
        return result

    def model_to_string(self) -> str:
        return "tree\nversion=v4\nfeature_names=" + " ".join(self.names) + "\n"


@dataclass
class FakeFit:
    """Records what the script trains on, and returns a FakeModel."""

    column: str = "amount"
    calls: list[tuple[npt.NDArray[np.float64], npt.NDArray[np.bool_], tuple[str, ...]]] = field(
        default_factory=list
    )

    def __call__(
        self, x: npt.NDArray[np.float64], fraud: npt.NDArray[np.bool_], names: Sequence[str]
    ) -> FakeModel:
        self.calls.append((x.copy(), fraud.copy(), tuple(names)))
        return FakeModel(names, self.column)
