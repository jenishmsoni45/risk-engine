"""make model's trainer with LightGBM itself, in the image, the only place LightGBM runs (D51.4,
D56f): two runs on one generated CSV write the same model file, metadata and card (Phase 7,
criterion 5, which `make model` checks by hand on the real CSV), and the model file names exactly
the 31 inputs (criterion 2)."""

from __future__ import annotations

import json
from pathlib import Path

from ml.train import META_FILE, MODEL_FILE
from risk.features.compute import FEATURE_NAMES
from tests.model_fakes import generated_csv, train_in_image


def test_two_runs_in_the_image_write_the_same_model(tmp_path: Path) -> None:
    csv = generated_csv(tmp_path)
    first, second = (train_in_image(tmp_path, csv, run) for run in ("a", "b"))
    for name in (f"model/{MODEL_FILE}", f"model/{META_FILE}", "model_card.md"):
        assert (first / name).read_bytes() == (second / name).read_bytes(), name

    text = (first / "model" / MODEL_FILE).read_text()
    names = [line for line in text.splitlines() if line.startswith("feature_names=")]
    assert names == ["feature_names=" + " ".join(FEATURE_NAMES)]
    meta = json.loads((first / "model" / META_FILE).read_text())
    assert meta["inputs"] == list(FEATURE_NAMES)
    assert "LightGBM 4.7.0" in (first / "model_card.md").read_text()
