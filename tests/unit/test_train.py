"""The training script (ml/train.py) on generated data, with a stand-in for LightGBM, which only
the image imports (D51.4): what it trains on, the files it writes, the model card (Phase 7,
criterion 6) and what it refuses. tests/integration/test_model_build.py trains LightGBM itself."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import numpy.typing as npt
import pytest

from ml.dataset import INPUTS, arrays
from ml.generate import write_csv
from ml.paysim import load
from ml.split import masks
from ml.train import CARD, META_FILE, MODEL_FILE, TrainError, build, main
from tests.model_fakes import FakeFit, FakeModel, generated_csv


@pytest.fixture(scope="module")
def csv(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return generated_csv(tmp_path_factory.mktemp("paysim"))


def outputs(folder: Path) -> tuple[Path, Path]:
    return folder / "model", folder / "model_card.md"


def test_trains_once_on_the_training_days_only(csv: Path, tmp_path: Path) -> None:
    fit = FakeFit()
    result = build(csv, *outputs(tmp_path), fit=fit)
    data = arrays(load(csv))
    train = masks(data.day)["train"]
    [(x, fraud, names)] = fit.calls
    assert names == INPUTS
    np.testing.assert_array_equal(x, data.x[train])
    np.testing.assert_array_equal(fraud, data.fraud[train])
    assert result.sets["train"].transfers == int(train.sum())
    assert result.sets["train"].last_day < result.sets["threshold"].first_day


def test_writes_the_model_and_its_metadata(csv: Path, tmp_path: Path) -> None:
    model_dir, card_path = outputs(tmp_path)
    result = build(csv, model_dir, card_path, fit=FakeFit())
    text = (model_dir / MODEL_FILE).read_bytes()
    assert text == FakeModel(INPUTS, "amount").model_to_string().encode()
    digest = hashlib.sha256(text).hexdigest()
    assert result.model_sha256 == digest
    assert result.name == f"lgbm-{digest[:12]}"
    meta = json.loads((model_dir / META_FILE).read_text())
    assert meta["name"] == result.name
    assert meta["file"] == MODEL_FILE
    assert meta["sha256"] == digest
    assert meta["inputs"] == list(INPUTS)
    assert meta["threshold"] == result.threshold
    assert meta["data"]["file"] == csv.name
    assert meta["days"] == {"train": [0, 2], "threshold": [3, 4], "test": [5, 11]}


def test_the_card_states_what_criterion_6_asks(csv: Path, tmp_path: Path) -> None:
    model_dir, card_path = outputs(tmp_path)
    result = build(csv, model_dir, card_path, fit=FakeFit())
    card = card_path.read_text()
    for part in (
        "## Data",
        result.csv_sha256,
        "## Split (D38)",
        "| train | 0 to 2 |",
        "| threshold | 3 to 4 |",
        "| test | 5 to 11 |",
        "## Inputs (D35, D54)",
        "## Threshold (D34, D55.3)",
        f"at least `{result.threshold!r}`",
        f"PR-AUC, as average precision: {result.pr_auc:.4f}",
        "| recall |",
        "| fraud value caught |",
        "| false-positive rate |",
        "## Limitations",
    ):
        assert part in card, part
    for name in INPUTS:
        assert f"`{name}`" in card
    assert "hour" not in card.split("## Inputs")[1].split("Not inputs")[0]


def test_two_builds_from_one_csv_write_the_same_files(csv: Path, tmp_path: Path) -> None:
    runs = [outputs(tmp_path / run) for run in ("a", "b")]
    for model_dir, card_path in runs:
        build(csv, model_dir, card_path, fit=FakeFit())
    (a_dir, a_card), (b_dir, b_card) = runs
    for name in (MODEL_FILE, META_FILE):
        assert (a_dir / name).read_bytes() == (b_dir / name).read_bytes()
    assert a_card.read_bytes() == b_card.read_bytes()


class ConstantModel(FakeModel):
    def predict(self, data: npt.NDArray[np.float64], /) -> npt.NDArray[np.float64]:
        return np.full(data.shape[0], 0.5)


def test_a_model_whose_top_score_is_shared_by_too_many_holds_none(
    csv: Path, tmp_path: Path
) -> None:
    def fit(*_: object) -> ConstantModel:
        return ConstantModel(INPUTS, "amount")

    model_dir, card_path = outputs(tmp_path)
    result = build(csv, model_dir, card_path, fit=fit)
    assert result.threshold_held == 0
    assert result.test.held == 0
    assert json.loads((model_dir / META_FILE).read_text())["threshold"] is None
    assert "- Threshold: none." in card_path.read_text()


def test_data_too_short_for_the_split_is_refused(tmp_path: Path) -> None:
    short = generated_csv(tmp_path, steps=30)
    with pytest.raises(TrainError, match="at least 10 days"):
        build(short, *outputs(tmp_path), fit=FakeFit())


def test_a_file_the_loader_refuses_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "backwards.csv"
    row = ("{step}", "TRANSFER", "10.00", "C1", "0", "0", "C2", "0", "0", "0", "0")
    write_csv(path, [tuple(v.format(step=step) for v in row) for step in (2, 1)])
    with pytest.raises(TrainError, match="the loader refuses"):
        build(path, *outputs(tmp_path), fit=FakeFit())


def transfer_row(step: int, fraud: bool, n: int) -> tuple[str, ...]:
    return (str(step), "TRANSFER", f"{10 + n}.00", f"C{n}", "0", "0", f"C{n + 1000}", "0", "0",
            "1" if fraud else "0", "0")  # fmt: skip


def test_threshold_days_without_a_transfer_are_refused(tmp_path: Path) -> None:
    # Transfers on day 0 and day 11 only: days 3 and 4, the threshold days, hold none.
    path = tmp_path / "gap.csv"
    write_csv(path, [transfer_row(step, n % 2 == 0, n) for n, step in enumerate((1, 1, 265, 265))])
    with pytest.raises(TrainError, match="the threshold days hold no transfer"):
        build(path, *outputs(tmp_path), fit=FakeFit())


@pytest.mark.parametrize("train_fraud", [False, True])
def test_training_days_need_both_fraud_and_other_transfers(
    tmp_path: Path, train_fraud: bool
) -> None:
    # Days 0 (training), 3 (threshold) and 11 (test); every training transfer is one kind.
    steps = [(1, train_fraud), (1, train_fraud), (73, False), (265, True), (265, False)]
    path = tmp_path / "one-kind.csv"
    write_csv(path, [transfer_row(step, fraud, n) for n, (step, fraud) in enumerate(steps)])
    with pytest.raises(TrainError, match="the training days need both"):
        build(path, *outputs(tmp_path), fit=FakeFit())


def test_test_days_need_both_fraud_and_other_transfers(tmp_path: Path) -> None:
    steps = [(1, True), (1, False), (73, False), (265, False), (265, False)]
    path = tmp_path / "no-test-fraud.csv"
    write_csv(path, [transfer_row(step, fraud, n) for n, (step, fraud) in enumerate(steps)])
    with pytest.raises(TrainError, match="the test days need both"):
        build(path, *outputs(tmp_path), fit=FakeFit())


def test_a_file_with_no_transfer_to_load_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "payments.csv"
    write_csv(path, [("1", "PAYMENT", "10.00", "C1", "0", "0", "M2", "0", "0", "0", "0")])
    with pytest.raises(TrainError, match="no transfer to load"):
        build(path, *outputs(tmp_path), fit=FakeFit())


def test_the_command_says_why_it_wrote_nothing(tmp_path: Path) -> None:
    model_dir, card_path = outputs(tmp_path)
    args = ["--model-dir", str(model_dir), "--card", str(card_path)]
    assert main(["--csv", str(tmp_path / "missing.csv"), *args]) == 2
    assert main(["--csv", str(generated_csv(tmp_path, steps=30)), *args]) == 1
    assert not model_dir.exists()
    assert not card_path.exists()


def test_the_card_goes_to_ml_reports() -> None:
    assert CARD.relative_to(CARD.parents[2]).as_posix() == "ml/reports/model_card.md"
