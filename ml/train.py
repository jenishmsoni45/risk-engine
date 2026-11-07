"""`make model`: train the LightGBM model on PaySim and write its files and model card
(design.md section 8; D30 confirmed in D55.1; D34, D38, D54, D55, D56).

1. The loader's transfers get their 31 features from the offline pipeline (D35, D54).
2. D38's split by days: the training days train, the threshold days set the threshold, and the
   test days stay unseen until the model is scored on them for the card.
3. LightGBM trains on the training days only, with fixed settings and one thread, so two runs
   write the same model (D56c).
4. The threshold holds at most 1% of the threshold days' transfers (D34, D55.3).
5. The test days give the card's PR-AUC, and the recall, fraud value caught and false-positive
   rate at that threshold.

It writes `ml/model/model.txt` (LightGBM's text format), `ml/model/model.json` (the name, inputs
and threshold that Phase 8's shadow scorer reads) and `ml/reports/model_card.md`; every number in
the card comes from this script (D29). LightGBM is imported only inside `lightgbm_fit`, which runs
only in the image (D51.4): `make model` runs this module in the `model` Compose service (D56a).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from importlib import metadata
from pathlib import Path
from typing import Any, Protocol

import numpy as np
import numpy.typing as npt

from ml.dataset import INPUTS, Arrays, arrays
from ml.metrics import AtThreshold, at_threshold, average_precision
from ml.paysim import PaySimError, load
from ml.profile import sha256, share, table
from ml.split import PARTS, Part, masks
from ml.threshold import Scores, capacity, held, threshold_for

ROOT = Path(__file__).resolve().parent.parent
MODEL_DIR = ROOT / "ml" / "model"
CARD = ROOT / "ml" / "reports" / "model_card.md"
MODEL_FILE = "model.txt"
META_FILE = "model.json"

# D56c: fixed settings, no search. One thread and forced row-wise histograms leave nothing to
# timing, so two runs on one CSV write the same model.
ROUNDS = 300
PARAMS: dict[str, Any] = {
    "objective": "binary",
    "learning_rate": 0.05,
    "num_leaves": 31,
    "min_data_in_leaf": 100,
    "seed": 7,
    "num_threads": 1,
    "deterministic": True,
    "force_row_wise": True,
    "verbosity": -1,
}


class TrainError(Exception):
    """No model can be trained from the file; the message says why."""


class Model(Protocol):
    """What the script needs from a trained model: LightGBM's Booster has both."""

    def predict(self, data: npt.NDArray[np.float64], /) -> Any: ...

    def model_to_string(self) -> str: ...


Fit = Callable[[npt.NDArray[np.float64], npt.NDArray[np.bool_], Sequence[str]], Model]


def lightgbm_fit(
    x: npt.NDArray[np.float64], fraud: npt.NDArray[np.bool_], names: Sequence[str]
) -> Model:
    """LightGBM on the training rows, with the features named so the model file lists them."""
    import lightgbm  # only inside the image, never on the Mac (D51.4)

    data = lightgbm.Dataset(x, label=fraud.astype(np.int8), feature_name=list(names))
    return lightgbm.train(PARAMS, data, num_boost_round=ROUNDS)


def scores(model: Model, x: npt.NDArray[np.float64]) -> Scores:
    result = np.asarray(model.predict(x), dtype=np.float64)
    if result.shape != (x.shape[0],):
        raise TrainError(f"the model gave scores of shape {result.shape} for {x.shape[0]} rows")
    return result


@dataclass(frozen=True)
class SetSummary:
    first_day: int
    last_day: int
    transfers: int
    fraud: int


@dataclass(frozen=True)
class Result:
    csv_name: str
    csv_sha256: str
    sets: dict[Part, SetSummary]
    name: str
    model_sha256: str
    lightgbm_version: str
    threshold: float  # math.inf when no score holds at most the capacity (D55.3)
    capacity: int
    threshold_held: int
    pr_auc: float
    test: AtThreshold


def _summary(rows: Arrays) -> SetSummary:
    return SetSummary(int(rows.day.min()), int(rows.day.max()), len(rows), int(rows.fraud.sum()))


def _lightgbm_version() -> str:
    try:
        return metadata.version("lightgbm")
    except metadata.PackageNotFoundError:
        return "not installed"


def build(
    csv: Path, model_dir: Path = MODEL_DIR, card_path: Path = CARD, fit: Fit = lightgbm_fit
) -> Result:
    """Trains on the file, writes the model, its metadata and the card, and returns the card's
    numbers."""
    digest = sha256(csv)
    try:
        data = arrays(load(csv))
    except PaySimError as exc:
        raise TrainError(f"the loader refuses {csv.name}: {exc}") from exc
    if len(data) == 0:
        raise TrainError(f"{csv.name} holds no transfer to load")
    try:
        split = masks(data.day)
    except ValueError as exc:
        raise TrainError(str(exc)) from exc
    parts = {part: data.rows(split[part]) for part in PARTS}
    del data, split  # the sets hold their own copies; free the whole file's array
    train, on_threshold, test = parts["train"], parts["threshold"], parts["test"]
    if len(on_threshold) == 0:
        raise TrainError("the threshold days hold no transfer")
    for part, rows in (("training", train), ("test", test)):
        if not rows.fraud.any() or rows.fraud.all():
            raise TrainError(f"the {part} days need both fraud and other transfers")

    model = fit(train.x, train.fraud, INPUTS)

    threshold_scores = scores(model, on_threshold.x)
    limit = capacity(len(on_threshold))
    threshold = threshold_for(threshold_scores, limit)
    test_scores = scores(model, test.x)
    text = model.model_to_string().encode()
    model_sha256 = hashlib.sha256(text).hexdigest()
    result = Result(
        csv_name=csv.name,
        csv_sha256=digest,
        sets={part: _summary(parts[part]) for part in PARTS},
        name=f"lgbm-{model_sha256[:12]}",
        model_sha256=model_sha256,
        lightgbm_version=_lightgbm_version(),
        threshold=threshold,
        capacity=limit,
        threshold_held=int(held(threshold_scores, threshold).sum()),
        pr_auc=average_precision(test_scores, test.fraud),
        test=at_threshold(test_scores, test.fraud, test.amount, threshold),
    )
    model_dir.mkdir(parents=True, exist_ok=True)
    (model_dir / MODEL_FILE).write_bytes(text)
    (model_dir / META_FILE).write_text(json.dumps(meta(result), indent=2) + "\n")
    card_path.parent.mkdir(parents=True, exist_ok=True)
    card_path.write_text(card(result, model_dir))
    return result


def meta(result: Result) -> dict[str, Any]:
    """model.json: what the shadow scorer needs besides the trees (Phase 8, D39)."""
    return {
        "name": result.name,
        "file": MODEL_FILE,
        "sha256": result.model_sha256,
        # float64 values, in this order (ml/dataset.py)
        "inputs": list(INPUTS),
        # held when the score is at or above it; null holds none (D55.3)
        "threshold": None if math.isinf(result.threshold) else result.threshold,
        "data": {"file": result.csv_name, "sha256": result.csv_sha256},
        "days": {
            part: [summary.first_day, summary.last_day] for part, summary in result.sets.items()
        },
    }


def _shown(path: Path) -> str:
    return str(path.relative_to(ROOT)) if path.is_relative_to(ROOT) else path.name


def _days(summary: SetSummary) -> str:
    return f"{summary.first_day} to {summary.last_day}"


def _threshold_lines(result: Result) -> list[str]:
    on = result.sets["threshold"].transfers
    lines = [
        f"- Review capacity: at most 1% of the threshold days' {on} transfers, that is "
        f"{result.capacity}.",
    ]
    if math.isinf(result.threshold):
        lines.append(
            f"- Threshold: none. More than {result.capacity} transfers share the top score, so "
            "no score holds at most the capacity, and the model holds nothing (D55.3)."
        )
    else:
        lines.append(
            f"- Threshold: a transfer is held when its score is at least `{result.threshold!r}`. "
            f"On the threshold days it holds {result.threshold_held} transfers "
            f"({share(result.threshold_held, on)})."
        )
    return lines


def card(result: Result, model_dir: Path = MODEL_DIR) -> str:
    """The model card (Phase 7, criterion 6): data, split, inputs, threshold, metrics, limits."""
    total = sum(s.transfers for s in result.sets.values())
    fraud = sum(s.fraud for s in result.sets.values())
    test = result.test
    settings = ", ".join(f"`{key}={value}`" for key, value in PARAMS.items())
    lines = [
        f"# Model card: {result.name}",
        "",
        "Written by `make model` (`python -m ml.train`) together with the model files; every "
        "number below comes from that script (D29). The model is scored in shadow next to the "
        "rules and never changes a decision (Phase 8, I8).",
        "",
        "## Model",
        "",
        f"- Name: `{result.name}`, from the first 12 hex digits of the model file's SHA-256 (D56b)",
        f"- Files: `{_shown(model_dir / MODEL_FILE)}` (LightGBM's text format), SHA-256 "
        f"`{result.model_sha256}`, and `{_shown(model_dir / META_FILE)}` (name, inputs, "
        "threshold)",
        f"- LightGBM {result.lightgbm_version}, trained in this repo's image only (D51.4), with "
        f"fixed settings and no search (D56c): {ROUNDS} rounds, {settings}. No early stopping "
        "and no class weights.",
        "",
        "## Data",
        "",
        f"- `{result.csv_name}`, SHA-256 `{result.csv_sha256}`: PaySim, a simulated month of "
        "mobile-money transactions, which stays in the git-ignored `data/` (CLAUDE.md)",
        f"- Loaded as ledger transfers (D32, D33, D40p): the TRANSFER and CASH_OUT rows of at "
        f"least 1 cent, {total} transfers, of which fraud {fraud} ({share(fraud, total)})",
        "- Label: PaySim's `isFraud`. `isFlaggedFraud` and the balance columns are never read "
        "(D35).",
        "",
        "## Split (D38)",
        "",
        "By day of the transfer's time, from 2026-01-01 UTC (D51.2). The model trains on the "
        "training days only; the threshold days only set the threshold, and the test days are "
        "scored once, for this card.",
        "",
        *table(
            ("set", "days", "transfers", "fraud", "fraud share"),
            (
                (part, _days(s), s.transfers, s.fraud, share(s.fraud, s.transfers))
                for part, s in result.sets.items()
            ),
        ),
        "",
        "## Inputs (D35, D54)",
        "",
        "The 31 features of D19, computed by the shared feature module (`risk/features/`) from "
        "strictly earlier transfers, as the live guard computes them, in this order:",
        "",
        ", ".join(f"`{name}`" for name in INPUTS),
        "",
        "Not inputs: the hour of day (D54), the transaction type, the balance columns, the "
        "account names and `isFlaggedFraud` (D35).",
        "",
        "## Threshold (D34, D55.3)",
        "",
        *_threshold_lines(result),
        "",
        "## On the test days",
        "",
        f"- PR-AUC, as average precision: {result.pr_auc:.4f}. A model that ranks at random "
        f"scores about the test days' fraud share, {share(test.fraud, test.transfers)}.",
        "- At the threshold, which holds at most 1% of the threshold days' transfers:",
        "",
        *table(
            ("measure", "value", "from"),
            (
                (
                    "transfers held",
                    share(test.held, test.transfers),
                    f"{test.held} of {test.transfers}",
                ),
                (
                    "recall",
                    share(test.fraud_held, test.fraud),
                    f"{test.fraud_held} of {test.fraud} fraud transfers",
                ),
                (
                    "fraud value caught",
                    share(test.fraud_cents_held, test.fraud_cents),
                    f"{test.fraud_cents_held} of {test.fraud_cents} cents",
                ),
                (
                    "false-positive rate",
                    share(test.not_fraud_held, test.not_fraud),
                    f"{test.not_fraud_held} of {test.not_fraud} other transfers",
                ),
            ),
        ),
        "",
        "## Limitations",
        "",
        "- PaySim is simulated. Its fraud follows the simulator's pattern, so these numbers say "
        "how well the model finds that pattern, not how it would do on real fraud.",
        "- PaySim's senders almost never repeat (`ml/reports/paysim_profile.md`, D54), so the "
        "sender-side features are nearly always empty and the model leans on the amount and the "
        "receiver's history. Where senders repeat, as in live traffic, it is untested.",
        "- It sees only what the live guard sees: no balance columns, transaction type or hour "
        "of day (D35, D54). That makes it weaker on PaySim than a model that reads them, by "
        "design: those columns make PaySim artificially easy.",
        "- Amounts are PaySim's unnamed currency read as CAD cents (D40p), so its splits on "
        "amounts and sums mean nothing for real CAD transfers.",
        "- The threshold is set on two days, so the share it holds on the test days differs "
        "from 1% (above).",
        "- Its scores rank transfers; they are not calibrated probabilities, and only their "
        "order is used.",
        "- Fixed settings and one training run, with no search, retraining or drift handling "
        "(out of scope): a tuned or retrained model may do better.",
        "",
    ]
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--csv", type=Path, required=True, help="a PaySim CSV")
    parser.add_argument("--model-dir", type=Path, default=MODEL_DIR)
    parser.add_argument("--card", type=Path, default=CARD)
    args = parser.parse_args(argv)
    if not args.csv.is_file():
        print(f"{args.csv} doesn't exist: put the PaySim CSV in data/ (CLAUDE.md)")
        return 2
    try:
        result = build(args.csv, args.model_dir, args.card)
    except TrainError as exc:
        print(f"no model written: {exc}")
        return 1
    test = result.test
    print(
        f"{result.name}: PR-AUC {result.pr_auc:.4f}; at the threshold, recall "
        f"{share(test.fraud_held, test.fraud)}, fraud value caught "
        f"{share(test.fraud_cents_held, test.fraud_cents)}, false-positive rate "
        f"{share(test.not_fraud_held, test.not_fraud)}; wrote {args.model_dir} and {args.card}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
