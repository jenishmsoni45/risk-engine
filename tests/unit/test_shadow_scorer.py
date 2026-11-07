"""The shadow scorer on its own (design.md section 8; D57.1, D58), with stand-in models, since
LightGBM never loads on the Mac (D51.4): the checks a model must pass at startup, what counts as a
score, and what a failing, slow or missing model gives. tests/invariants/test_i8_shadow.py shows
that none of it changes a decision; tests/integration/test_shadow.py runs the real model."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from pathlib import Path
from typing import Any

import numpy as np
import pytest

from risk import shadow
from risk.features.compute import FEATURE_NAMES
from risk.shadow import META_FILE, Model, ModelError, Shadow, load, start, valid_score
from tests.feature_state import sample
from tests.helpers import ROOT
from tests.shadow_fakes import NONSENSE, StandIn, nonsense, raising, stand_in, working

FAILURES = "risk_shadow_failures_total"
COMMITTED = ROOT / "ml" / "model"
FEATURES = {name: n for n, name in enumerate(FEATURE_NAMES)}
TEXT = "tree\nversion=v4\nfeature_names=" + " ".join(FEATURE_NAMES) + "\n"


def write_model(folder: Path, text: str = TEXT, **meta: Any) -> Path:
    """A model folder like make model's: model.txt and model.json naming its SHA-256."""
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "model.txt").write_text(text)
    document = {
        "name": "lgbm-test",
        "file": "model.txt",
        "sha256": hashlib.sha256(text.encode()).hexdigest(),
        "inputs": list(FEATURE_NAMES),
        "threshold": 0.5,
    } | meta
    (folder / META_FILE).write_text(json.dumps(document))
    return folder


def from_text(text: str) -> StandIn:
    """A stand-in booster named as model.txt's feature_names line names its features, as LightGBM
    reads them."""
    line = next(line for line in text.splitlines() if line.startswith("feature_names="))
    return StandIn(working, line.removeprefix("feature_names=").split())


# --- Loading (D58b) ------------------------------------------------------------------------------


def test_a_good_folder_loads(tmp_path: Path) -> None:
    model = load(write_model(tmp_path), from_text)
    assert model.name == "lgbm-test"
    assert model.inputs == FEATURE_NAMES


def test_the_committed_model_passes_the_checks() -> None:
    """ml/model/ as committed: its model.json matches model.txt, and both name the 31 inputs."""
    model = load(COMMITTED, from_text)
    meta = json.loads((COMMITTED / META_FILE).read_text())
    assert model.name == meta["name"]
    assert meta["name"] == "lgbm-" + meta["sha256"][:12]


@pytest.mark.parametrize(
    ("meta", "message"),
    [
        ({"name": ""}, "name must be a non-empty string"),
        ({"name": 7}, "name must be a non-empty string"),
        ({"inputs": list(FEATURE_NAMES[:-1])}, "inputs must be the 31 features"),
        ({"inputs": list(reversed(FEATURE_NAMES))}, "inputs must be the 31 features"),
        ({"inputs": [*FEATURE_NAMES, "hour"]}, "inputs must be the 31 features"),
        ({"file": "../model.txt"}, "file must be a file name in the same folder"),
        ({"file": ""}, "file must be a file name in the same folder"),
        ({"file": "other.txt"}, "other.txt can't be read"),
        ({"sha256": "0" * 64}, "SHA-256 differs"),
    ],
)
def test_a_model_json_that_fails_a_check_is_refused(
    tmp_path: Path, meta: dict[str, Any], message: str
) -> None:
    with pytest.raises(ModelError, match=message):
        load(write_model(tmp_path, **meta), from_text)


@pytest.mark.parametrize("content", ["", "{", "[1, 2]", "\xff"])
def test_an_unreadable_model_json_is_refused(tmp_path: Path, content: str) -> None:
    write_model(tmp_path)
    (tmp_path / META_FILE).write_bytes(content.encode("latin-1"))
    with pytest.raises(ModelError, match=r"model\.json"):
        load(tmp_path, from_text)


def test_a_missing_folder_is_refused(tmp_path: Path) -> None:
    with pytest.raises(ModelError, match="can't be read"):
        load(tmp_path / "nothing", from_text)


def test_a_file_the_library_cant_read_is_refused(tmp_path: Path) -> None:
    def refuse(text: str) -> StandIn:
        raise ValueError("not a model file")

    with pytest.raises(ModelError, match="can't be loaded: ValueError"):
        load(write_model(tmp_path), refuse)


def test_a_model_trained_on_other_features_is_refused(tmp_path: Path) -> None:
    text = "tree\nfeature_names=" + " ".join(reversed(FEATURE_NAMES)) + "\n"
    with pytest.raises(ModelError, match="feature names aren't"):
        load(write_model(tmp_path, text), from_text)


# --- What counts as a score (D58d) ---------------------------------------------------------------


@pytest.mark.parametrize(
    "answer", [np.array([0.0]), np.array([1.0]), np.array([0.25], dtype=np.float32), 0.5]
)
def test_one_number_from_0_to_1_is_a_score(answer: object) -> None:
    assert valid_score(answer) == pytest.approx(float(np.asarray(answer).ravel()[0]))


@pytest.mark.parametrize("answer", NONSENSE, ids=repr)
def test_anything_else_is_not(answer: object) -> None:
    assert valid_score(answer) is None


# --- Scoring -------------------------------------------------------------------------------------


def test_a_working_model_gives_its_score_from_float64_rows_on_one_thread() -> None:
    def answer(rows: Any) -> object:
        return np.array([0.75]) if rows.dtype == np.float64 and rows.shape == (1, 31) else None

    booster = StandIn(answer)
    scorer = Shadow(Model("m", FEATURE_NAMES, booster), 0.5)
    try:
        assert asyncio.run(scorer.score(FEATURES)) == 0.75
    finally:
        scorer.close()
    assert booster.params == [{"num_threads": 1}]


def test_the_row_follows_the_models_input_order() -> None:
    seen: list[list[float]] = []

    def record(rows: Any) -> object:
        seen.append(rows.tolist()[0])
        return np.array([0.5])

    scorer = Shadow(Model("m", FEATURE_NAMES, StandIn(record)), 0.5)
    try:
        asyncio.run(scorer.score(FEATURES))
    finally:
        scorer.close()
    assert seen == [[float(FEATURES[name]) for name in FEATURE_NAMES]]


@pytest.mark.parametrize(
    ("answer", "reason"), [(raising, "error"), (nonsense, "invalid")], ids=["raises", "nonsense"]
)
def test_a_failing_model_gives_no_score_and_is_counted(answer: Any, reason: str) -> None:
    before = sample(FAILURES, reason=reason)
    scorer = stand_in(answer)
    try:
        assert asyncio.run(scorer.score(FEATURES)) is None
    finally:
        scorer.close()
    assert sample(FAILURES, reason=reason) == before + 1


def test_a_slow_model_gives_no_score_within_its_limit_and_runs_nothing_stale() -> None:
    """A score past SHADOW_TIMEOUT_MS is dropped and counted (D57.1); requests that queued
    behind the slow one are cancelled before they run, so they never reach the model."""
    calls: list[float] = []

    def sleepy(rows: Any) -> object:
        calls.append(time.monotonic())
        time.sleep(0.3)
        return np.array([0.5])

    before = sample(FAILURES, reason="timeout")
    scorer = Shadow(Model("m", FEATURE_NAMES, StandIn(sleepy)), 0.02)

    async def three() -> tuple[list[float | None], float]:
        started = time.monotonic()
        scores = await asyncio.gather(*(scorer.score(FEATURES) for _ in range(3)))
        return list(scores), time.monotonic() - started

    try:
        scores, took = asyncio.run(three())
        time.sleep(0.4)  # the first call finishes; the two queued behind it must not start
    finally:
        scorer.close()
    assert scores == [None, None, None]
    assert took < 0.02 + 0.1
    assert len(calls) == 1
    assert sample(FAILURES, reason="timeout") == before + 3


def test_no_model_loaded_gives_no_score_and_is_counted() -> None:
    before = sample(FAILURES, reason="unavailable")
    scorer = Shadow(None, 0.01)
    try:
        assert scorer.model_name is None
        assert asyncio.run(scorer.score(FEATURES)) is None
    finally:
        scorer.close()
    assert sample(FAILURES, reason="unavailable") == before + 1


def test_a_failure_is_logged_once_per_kind(caplog: pytest.LogCaptureFixture) -> None:
    scorer = stand_in(raising)
    try:
        with caplog.at_level(logging.WARNING, logger=shadow.__name__):
            for _ in range(3):
                asyncio.run(scorer.score(FEATURES))
    finally:
        scorer.close()
    assert len([r for r in caplog.records if "raised" in r.getMessage()]) == 1


def test_a_closed_scorer_gives_no_score() -> None:
    scorer = stand_in(working)
    scorer.close()
    assert asyncio.run(scorer.score(FEATURES)) is None


# --- Starting (D57.5, D58c) ----------------------------------------------------------------------


def test_off_is_no_scorer() -> None:
    assert start(None, 0.01, from_text) is None


def test_a_good_model_starts_scoring(tmp_path: Path) -> None:
    scorer = start(write_model(tmp_path), 0.01, from_text)
    assert scorer is not None
    try:
        assert scorer.model_name == "lgbm-test"
    finally:
        scorer.close()


def test_a_bad_model_starts_without_one_and_says_why(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.ERROR, logger=shadow.__name__):
        scorer = start(write_model(tmp_path, sha256="0" * 64), 0.01, from_text)
    assert scorer is not None
    try:
        assert scorer.model is None
    finally:
        scorer.close()
    assert "shadow model not loaded" in caplog.text
    assert "SHA-256 differs" in caplog.text
