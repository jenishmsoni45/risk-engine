"""The shadow scorer (design.md section 8; D39, D57.1, D58).

At startup the decision API loads the model in SHADOW_MODEL_DIR and checks it (D58b). A model that
fails a check is logged and not used, and the API starts anyway: decisions never depend on it
(design.md section 7). After the rules have decided, the decision path asks `Shadow.score` for a
score of the 31 features. One worker thread runs the model; the answer is the score, or None with
the reason counted, when the model raises, gives anything but one finite number from 0 to 1, or
takes longer than SHADOW_TIMEOUT_MS. The decision is fixed before `score` is called and never reads
what it returns (I8).

A request whose wait has ended is cancelled while it is still queued, so a slow model runs at
most one stale request at a time and can't pile up work.

LightGBM is imported only in `lightgbm_booster`, which runs only in the image (D51.4).
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import hashlib
import json
import logging
import math
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import numpy as np
import numpy.typing as npt
from prometheus_client import Counter

from risk.features.compute import FEATURE_NAMES

log = logging.getLogger(__name__)

META_FILE = "model.json"
REASONS = ("unavailable", "error", "invalid", "timeout")

FAILURES = Counter(
    "risk_shadow_failures",
    "Decisions stored without a shadow score while shadow scoring is on, by reason: unavailable "
    "(no model loaded), error (the model raised), invalid (not one number from 0 to 1) or timeout",
    ["reason"],
)
for _reason in REASONS:
    FAILURES.labels(_reason)


class Booster(Protocol):
    """What the scorer needs from a model: LightGBM's Booster has both."""

    def predict(self, data: npt.NDArray[np.float64], /, *, num_threads: int) -> Any: ...

    def feature_name(self) -> list[str]: ...


class ModelError(Exception):
    """The model can't be used; the message says why."""


def lightgbm_booster(text: str) -> Booster:
    """A LightGBM Booster from the text of model.txt."""
    import lightgbm  # only inside the image, never on the Mac (D51.4)

    booster: Booster = lightgbm.Booster(model_str=text)
    return booster


@dataclass(frozen=True)
class Model:
    name: str
    inputs: tuple[str, ...]
    booster: Booster

    def predict(self, features: Mapping[str, int]) -> Any:
        """The booster's raw answer for one transfer. float64, because float32 can't hold every
        amount in cents (ml/dataset.py); one thread, because OpenMP's start-up costs more than a
        single row (D58h)."""
        row = np.array([[features[name] for name in self.inputs]], dtype=np.float64)
        return self.booster.predict(row, num_threads=1)


def load(directory: Path, booster_from: Callable[[str], Booster] = lightgbm_booster) -> Model:
    """The model in directory, after D58b's checks; ModelError says which one failed."""
    meta_path = directory / META_FILE
    try:
        meta: Any = json.loads(meta_path.read_text())
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise ModelError(f"{meta_path} can't be read: {exc}") from None
    if not isinstance(meta, dict):
        raise ModelError(f"{meta_path} must hold a JSON object")
    name, file, digest = meta.get("name"), meta.get("file"), meta.get("sha256")
    if not isinstance(name, str) or not name:
        raise ModelError(f"{meta_path}: name must be a non-empty string")
    if meta.get("inputs") != list(FEATURE_NAMES):
        raise ModelError(f"{meta_path}: inputs must be the 31 features, in FEATURE_NAMES order")
    # A bare file name, so model.json can't point outside its folder.
    if not isinstance(file, str) or not file or Path(file).name != file:
        raise ModelError(f"{meta_path}: file must be a file name in the same folder")
    try:
        data = (directory / file).read_bytes()
    except OSError as exc:
        raise ModelError(f"{directory / file} can't be read: {exc}") from None
    if hashlib.sha256(data).hexdigest() != digest:
        raise ModelError(f"{directory / file}'s SHA-256 differs from the one in {meta_path}")
    try:
        booster = booster_from(data.decode())
        names = list(booster.feature_name())
    # Whatever the library raises on a file it can't read, the API must still start.
    except Exception as exc:  # noqa: BLE001
        raise ModelError(f"{directory / file} can't be loaded: {exc!r}") from None
    if names != list(FEATURE_NAMES):
        raise ModelError(f"{directory / file}'s feature names aren't {META_FILE}'s inputs")
    return Model(name, FEATURE_NAMES, booster)


def valid_score(result: object) -> float | None:
    """The score in a model's answer, when the answer is one finite number from 0 to 1 (D58d):
    a float, or a one-element array of floats, as LightGBM returns for one row. None otherwise."""
    if isinstance(result, np.ndarray):
        if result.shape != (1,) or not np.issubdtype(result.dtype, np.floating):
            return None
        value = float(result[0])
    elif isinstance(result, float | np.floating):
        value = float(result)
    else:
        return None
    return value if math.isfinite(value) and 0.0 <= value <= 1.0 else None


class Shadow:
    """Shadow scoring while it is on: a loaded model, or None when it failed to load (D58c)."""

    def __init__(self, model: Model | None, timeout_s: float) -> None:
        self.model = model
        self.timeout_s = timeout_s
        self._worker = concurrent.futures.ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="shadow"
        )
        self._logged: set[str] = set()

    @property
    def model_name(self) -> str | None:
        return None if self.model is None else self.model.name

    def _failed(self, reason: str, message: str, *args: object) -> None:
        """Counts a failure; logs only the first of each reason, since a broken model fails on
        every decision and the counter carries the rest."""
        FAILURES.labels(reason).inc()
        if reason not in self._logged:
            self._logged.add(reason)
            log.warning(message + "; later failures of this kind are only counted", *args)

    async def score(self, features: Mapping[str, int]) -> float | None:
        """The model's score of the features, or None with the reason counted. Never raises,
        except to pass on the cancellation of the request that asked."""
        if self.model is None:
            FAILURES.labels("unavailable").inc()
            return None
        try:
            job = self._worker.submit(self.model.predict, features)
            # On timeout, or when the request is cancelled, wait_for cancels the wrapper, which
            # cancels the job if it hasn't started.
            result = await asyncio.wait_for(asyncio.wrap_future(job), self.timeout_s)
        except TimeoutError:
            self._failed(
                "timeout", "shadow model %s took over %s s", self.model.name, self.timeout_s
            )
            return None
        # Anything the model raises is its failure, never the decision's (I8).
        except Exception as exc:  # noqa: BLE001
            self._failed("error", "shadow model %s raised %r", self.model.name, exc)
            return None
        score = valid_score(result)
        if score is None:
            self._failed(
                "invalid",
                "shadow model %s gave %r, not a score from 0 to 1",
                self.model.name,
                result,
            )
        return score

    def close(self) -> None:
        """Stops the worker; a score still running finishes on its own."""
        self._worker.shutdown(wait=False, cancel_futures=True)


def start(
    directory: Path | None,
    timeout_s: float,
    booster_from: Callable[[str], Booster] = lightgbm_booster,
) -> Shadow | None:
    """Shadow scoring from SHADOW_MODEL_DIR: None when it is off, and a Shadow without a model
    when the model fails a check, which is logged."""
    if directory is None:
        log.info("shadow scoring is off")
        return None
    try:
        model = load(directory, booster_from)
    except ModelError as exc:
        log.error("shadow model not loaded; decisions are stored without a score: %s", exc)
        return Shadow(None, timeout_s)
    log.info("shadow model %s loaded from %s", model.name, directory)
    return Shadow(model, timeout_s)
