"""`make backtest`: the PaySim rule set against the model on the test days (design.md section 8;
D40q, D54, D57.3, D58k).

1. The loader's transfers get their 31 features from the offline pipeline, and D38's split picks
   the test days, which neither the model nor the rule set has seen.
2. The PaySim rule set (`ml/rules/paysim.yaml`, D54) decides each test transfer through the
   service's own evaluation function. A hold and a block both stop the money, so both count as
   held (D54).
3. The model in `ml/model/`, checked as the decision API checks it (D58b), scores each test
   transfer.
4. Three lines, each with recall, fraud value caught and false-positive rate: the rules at their
   own hold rate; the model holding at most as many, its highest scores; and the model holding
   at most 1% of the test days' transfers. The model's lines are drawn on the test days' scores
   by D55.3's rule, so a tie at a line makes it hold fewer, never more (D57.3).

The rule file and the model must both come from the CSV given: the backtest refuses a file whose
SHA-256 differs from the ones they name. It writes `ml/reports/backtest.md` and
`ml/reports/backtest.json`, from which `make report` writes README's 2b block (D57.4); every number
in either comes from this script (D29). LightGBM is imported only through `model_scores`, which
runs only in the image (D51.4): `make backtest` runs this module in the `backtest` Compose service.
"""

from __future__ import annotations

import argparse
import json
import re
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import numpy.typing as npt

from ml.dataset import INPUTS, arrays
from ml.metrics import AtThreshold, Cents, Flags, counts
from ml.paysim import PaySimError, load
from ml.profile import sha256, share, table
from ml.split import masks
from ml.threshold import Scores, capacity, held, threshold_for
from risk import shadow
from risk.rules import ruleset

ROOT = Path(__file__).resolve().parent.parent
RULES = ROOT / "ml" / "rules" / "paysim.yaml"
MODEL_DIR = ROOT / "ml" / "model"
REPORT = ROOT / "ml" / "reports" / "backtest.md"
DATA = ROOT / "ml" / "reports" / "backtest.json"
# The rule file's header names the CSV it was made from (ml/paysim_rules.py).
RULES_CSV = re.compile(r"^#.*\(SHA-256 ([0-9a-f]{64})\)", re.MULTILINE)
# float64 holds every whole number below 2^53 exactly (ml/dataset.py).
EXACT_LIMIT = 2**53

Rows = npt.NDArray[np.float64]
# The model's name and its scores of the rows, which hold INPUTS in order.
Scorer = Callable[[Path, Rows], tuple[str, Scores]]


class BacktestError(Exception):
    """No backtest can be run on the files given; the message says why."""


def model_scores(model_dir: Path, x: Rows) -> tuple[str, Scores]:
    """The model the decision API would load from model_dir, after the same checks, scoring
    every row."""
    try:
        model = shadow.load(model_dir)
    except shadow.ModelError as exc:
        raise BacktestError(str(exc)) from exc
    return model.name, np.asarray(model.booster.predict(x, num_threads=1), dtype=np.float64)


@dataclass(frozen=True)
class Line:
    """One line of the comparison, with what it holds counted."""

    who: str  # "rules" or "model"
    rate: str  # "own", "rules", or "1%": whose hold rate the line holds at most
    threshold: float | None  # the model's line; None for the rules, or when it holds nothing
    counts: AtThreshold


@dataclass(frozen=True)
class Result:
    csv_name: str
    csv_sha256: str
    rules_file: str
    rules_version: int
    rules_sha256: str
    model_name: str
    test_days: tuple[int, int]
    lines: tuple[Line, ...]


def _shown(path: Path) -> str:
    return str(path.relative_to(ROOT)) if path.is_relative_to(ROOT) else path.name


def _check_sources(digest: str, rules_path: Path, model_dir: Path) -> None:
    """The rule file and the model must both name this CSV."""
    try:
        named = RULES_CSV.search(rules_path.read_text())
    except (OSError, UnicodeDecodeError) as exc:
        raise BacktestError(f"{rules_path} can't be read: {exc}") from None
    if named is None or named.group(1) != digest:
        raise BacktestError(
            f"{rules_path.name} wasn't made from this CSV (SHA-256 {digest}): run "
            "`make paysim-rules` on it first"
        )
    meta_path = model_dir / shadow.META_FILE
    try:
        trained_on = json.loads(meta_path.read_text())["data"]["sha256"]
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise BacktestError(f"{meta_path} doesn't name the CSV it was trained on: {exc}") from None
    if trained_on != digest:
        raise BacktestError(
            f"the model wasn't trained on this CSV (SHA-256 {digest}): run `make model` on it first"
        )


def rule_holds(rules: ruleset.RuleSet, x: Rows) -> npt.NDArray[np.bool_]:
    """Which rows the rules hold or block, each row's features given back to the service's own
    evaluation function as the whole numbers they are."""
    if x.size and np.abs(x).max() >= EXACT_LIMIT:
        raise BacktestError("a feature is too large to hold exactly as float64")
    return np.array(
        [rules.evaluate(dict(zip(INPUTS, map(int, row), strict=True)))[0] != "allow" for row in x],
        dtype=np.bool_,
    )


def _model_line(rate: str, scores: Scores, at_most: int, fraud: Flags, amount: Cents) -> Line:
    """The model holding at most at_most transfers, by D55.3's rule on these scores."""
    threshold = threshold_for(scores, at_most)
    hold = held(scores, threshold)
    return Line(
        "model", rate, None if np.isinf(threshold) else threshold, counts(hold, fraud, amount)
    )


def run(
    csv: Path,
    rules_path: Path = RULES,
    model_dir: Path = MODEL_DIR,
    score: Scorer = model_scores,
) -> Result:
    """The comparison on the test days of csv."""
    digest = sha256(csv)
    _check_sources(digest, rules_path, model_dir)
    try:
        rules = ruleset.load(rules_path)
    except ruleset.RuleSetError as exc:
        raise BacktestError(str(exc)) from exc
    try:
        data = arrays(load(csv))
    except PaySimError as exc:
        raise BacktestError(f"the loader refuses {csv.name}: {exc}") from exc
    if len(data) == 0:
        raise BacktestError(f"{csv.name} holds no transfer to load")
    try:
        test = data.rows(masks(data.day)["test"])
    except ValueError as exc:
        raise BacktestError(str(exc)) from exc
    del data  # the test days hold their own copy; free the whole file's array
    if not test.fraud.any() or test.fraud.all():
        raise BacktestError("the test days need both fraud and other transfers")

    stopped = rule_holds(rules, test.x)
    name, scores = score(model_dir, test.x)
    if scores.shape != (len(test),) or not np.all(np.isfinite(scores)):
        raise BacktestError(f"the model gave no finite score for each of {len(test)} transfers")
    lines = (
        Line("rules", "own", None, counts(stopped, test.fraud, test.amount)),
        _model_line("rules", scores, int(stopped.sum()), test.fraud, test.amount),
        _model_line("1%", scores, capacity(len(test)), test.fraud, test.amount),
    )
    return Result(
        csv_name=csv.name,
        csv_sha256=digest,
        rules_file=_shown(rules_path),
        rules_version=rules.version,
        rules_sha256=rules.sha256,
        model_name=name,
        test_days=(int(test.day.min()), int(test.day.max())),
        lines=lines,
    )


def data(result: Result) -> dict[str, Any]:
    """backtest.json: everything README's 2b block shows."""
    document = asdict(result)
    document["test_days"] = list(result.test_days)
    document["lines"] = list(document["lines"])
    return document


def from_data(document: dict[str, Any]) -> Result:
    lines = tuple(
        Line(line["who"], line["rate"], line["threshold"], AtThreshold(**line["counts"]))
        for line in document["lines"]
    )
    return Result(
        **{key: value for key, value in document.items() if key not in ("lines", "test_days")},
        test_days=(document["test_days"][0], document["test_days"][1]),
        lines=lines,
    )


LABELS = {
    ("rules", "own"): "PaySim rules (v{version}), at their own hold rate",
    ("model", "rules"): "model `{model}`, at the rules' hold rate",
    ("model", "1%"): "model `{model}`, at 1%",
}


def comparison(result: Result) -> list[str]:
    """The comparison as a Markdown table, for the report and for README.md."""
    rows = []
    for line in result.lines:
        c = line.counts
        label = LABELS[(line.who, line.rate)].format(
            version=result.rules_version, model=result.model_name
        )
        rows.append(
            (
                label,
                f"{share(c.held, c.transfers)} ({c.held})",
                f"{share(c.fraud_held, c.fraud)} ({c.fraud_held} of {c.fraud})",
                share(c.fraud_cents_held, c.fraud_cents),
                f"{share(c.not_fraud_held, c.not_fraud)} ({c.not_fraud_held})",
            )
        )
    return table(("", "held", "recall", "fraud value caught", "false-positive rate"), rows)


def report(result: Result) -> str:
    first = result.lines[0].counts
    days = f"days {result.test_days[0]} to {result.test_days[1]}"
    return "\n".join(
        [
            "# Backtest: the PaySim rules against the model",
            "",
            "Written by `make backtest` (`python -m ml.backtest`) together with "
            "`ml/reports/backtest.json`; every number below comes from that script (D29). "
            "design.md section 8 and D40q set what it compares, and D57.3 sets the model's lines.",
            "",
            f"- Data: `{result.csv_name}`, SHA-256 `{result.csv_sha256}`",
            f"- Test days: {days} (D38), {first.transfers} transfers, of which fraud "
            f"{first.fraud}. Neither the rules nor the model saw them.",
            f"- Rules: `{result.rules_file}`, v{result.rules_version}, SHA-256 "
            f"`{result.rules_sha256}`, through the service's own evaluation function. A hold and "
            "a block both count as held (D54).",
            f"- Model: `{result.model_name}` from `ml/model/`, checked as the decision API checks "
            "it (D58b).",
            "",
            "## On the test days",
            "",
            *comparison(result),
            "",
            "- The rules hold what they hold; the model then holds at most as many of the test "
            "days' transfers, those it scores highest, and then at most 1% of them. Each line is "
            "the lowest score at which at most that many score at or above it, so transfers "
            "sharing the score at a line all stay below it, and the model holds fewer, never more "
            "(D55.3).",
            "- Recall is fraud held over all fraud; fraud value caught is the fraud amounts held "
            "over all fraud amounts; the false-positive rate is other transfers held over all "
            "other transfers.",
            "- The model's lines are drawn on the test days' own scores, so that both sides hold "
            "the same number. The threshold the model ships with, set on the threshold days, and "
            "what it holds on the test days are in `ml/reports/model_card.md`.",
            "- PaySim is simulated, and its amounts are read as CAD cents (D40p); the model "
            "card's limitations apply here too.",
            "",
        ]
    )


def build(
    csv: Path,
    rules_path: Path = RULES,
    model_dir: Path = MODEL_DIR,
    report_path: Path = REPORT,
    data_path: Path = DATA,
    score: Scorer = model_scores,
) -> Result:
    """Runs the backtest and writes its report and data file."""
    result = run(csv, rules_path, model_dir, score)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(report(result))
    data_path.parent.mkdir(parents=True, exist_ok=True)
    data_path.write_text(json.dumps(data(result), indent=2) + "\n")
    return result


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--csv", type=Path, required=True, help="a PaySim CSV")
    parser.add_argument("--rules", type=Path, default=RULES)
    parser.add_argument("--model-dir", type=Path, default=MODEL_DIR)
    parser.add_argument("--report", type=Path, default=REPORT)
    parser.add_argument("--data", type=Path, default=DATA)
    args = parser.parse_args(argv)
    if not args.csv.is_file():
        print(f"{args.csv} doesn't exist: put the PaySim CSV in data/ (CLAUDE.md)")
        return 2
    try:
        result = build(args.csv, args.rules, args.model_dir, args.report, args.data)
    except BacktestError as exc:
        print(f"no backtest written: {exc}")
        return 1
    for line in comparison(result):
        print(line)
    print(f"wrote {args.report} and {args.data}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
