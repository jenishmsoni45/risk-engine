"""The model's input list is exactly the 31 features (Phase 7, criterion 2; D35, D54): what the
script trains on, and what the committed model file and its metadata name. Read as text, so no
test imports LightGBM on the Mac (D51.4)."""

from __future__ import annotations

import hashlib
import json
from itertools import islice
from pathlib import Path

from ml.dataset import INPUTS
from ml.offline import features
from ml.paysim import load
from ml.train import META_FILE, MODEL_DIR, MODEL_FILE, build
from risk.features.compute import FEATURE_NAMES
from tests.model_fakes import FakeFit, generated_csv


def test_the_inputs_are_the_31_features_in_order_without_the_hour() -> None:
    assert INPUTS == FEATURE_NAMES
    assert len(INPUTS) == 31
    assert not any("hour" in name for name in INPUTS)


def test_each_training_row_is_the_offline_features_of_its_transfer_in_input_order(
    tmp_path: Path,
) -> None:
    csv = generated_csv(tmp_path)
    fit = FakeFit()
    build(csv, tmp_path / "model", tmp_path / "card.md", fit=fit)
    [(x, _, names)] = fit.calls
    assert names == FEATURE_NAMES
    assert x.shape[1] == 31
    # The training days come first in time order, so their rows are the first transfers.
    expected = [
        [row.features[name] for name in FEATURE_NAMES]
        for row in islice(features(load(csv)), x.shape[0])
    ]
    assert x.tolist() == expected


def model_text() -> str:
    return (MODEL_DIR / MODEL_FILE).read_text()


def test_the_committed_model_file_names_exactly_the_31_features() -> None:
    lines = [line for line in model_text().splitlines() if line.startswith("feature_names=")]
    assert lines == ["feature_names=" + " ".join(FEATURE_NAMES)]


def test_the_committed_metadata_names_the_inputs_and_the_model_file() -> None:
    meta = json.loads((MODEL_DIR / META_FILE).read_text())
    assert meta["inputs"] == list(FEATURE_NAMES)
    digest = hashlib.sha256((MODEL_DIR / MODEL_FILE).read_bytes()).hexdigest()
    assert meta["sha256"] == digest
    assert meta["name"] == f"lgbm-{digest[:12]}"
