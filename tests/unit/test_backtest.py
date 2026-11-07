"""The backtest CLI (design.md section 8; D40q, D57.3, D58k; Phase 8, criterion 3) on generated
PaySim-format data (D37), with stand-in scorers in place of LightGBM, which never loads on the Mac
(D51.4). The real report is `make backtest`'s, committed.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

from ml.backtest import BacktestError, Result, Rows, build, comparison, data, from_data, run
from ml.offline import features
from ml.paysim import load
from ml.profile import sha256
from ml.split import day_of, part_of
from ml.threshold import Scores, capacity
from risk.features.compute import FEATURE_NAMES
from risk.rules import ruleset
from tests.model_fakes import generated_csv

AMOUNT = FEATURE_NAMES.index("amount")


def rules_file(folder: Path, csv_digest: str, min_amount: int) -> Path:
    """A PaySim-style rule file naming its CSV in its header, as make paysim-rules writes it:
    hold every transfer of min_amount or more."""
    path = folder / "paysim.yaml"
    path.write_text(
        f"# Made from generated.csv (SHA-256 {csv_digest}).\n"
        "version: 2\ndefault: allow\non_timeout: hold\nrules:\n"
        "  - id: large_transfer\n    action: hold\n    all:\n"
        f'      - {{ feature: amount, op: ">=", value: {min_amount} }}\n'
    )
    return path


def model_dir(folder: Path, csv_digest: str) -> Path:
    folder.mkdir(exist_ok=True)
    (folder / "model.json").write_text(json.dumps({"data": {"sha256": csv_digest}}))
    return folder


def by_amount(model: Path, x: Rows) -> tuple[str, Scores]:
    """Ranks transfers by amount alone, as a rule on the amount does."""
    amounts = x[:, AMOUNT]
    return "stand-in-amount", amounts / (amounts.max() + 1)


def shuffled(model: Path, x: Rows) -> tuple[str, Scores]:
    """Distinct scores in an order unrelated to the features."""
    return "stand-in-shuffled", np.random.default_rng(7).permutation(len(x)) / len(x)


def tied(model: Path, x: Rows) -> tuple[str, Scores]:
    return "stand-in-tied", np.full(len(x), 0.5)


@pytest.fixture(scope="module")
def csv(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return generated_csv(tmp_path_factory.mktemp("backtest"))


def rows_on_test_days(csv: Path) -> list[tuple[int, bool, dict[str, int]]]:
    """(amount, fraud, features) of every test-day transfer, from the offline pipeline directly."""
    rows = list(features(load(csv)))
    last_day = day_of(rows[-1].transfer.time_ms)
    return [
        (row.transfer.amount, row.transfer.is_fraud, row.features)
        for row in rows
        if part_of(day_of(row.transfer.time_ms), last_day) == "test"
    ]


def median_test_amount(csv: Path) -> int:
    return int(np.median([amount for amount, _, _ in rows_on_test_days(csv)]))


def setup(tmp_path: Path, csv: Path, min_amount: int) -> tuple[Path, Path]:
    digest = sha256(csv)
    return rules_file(tmp_path, digest, min_amount), model_dir(tmp_path / "model", digest)


def test_the_rules_line_is_the_services_own_decisions_on_the_test_days(
    tmp_path: Path, csv: Path
) -> None:
    rules_path, model = setup(tmp_path, csv, median_test_amount(csv))
    result = run(csv, rules_path, model, shuffled)
    rules = ruleset.load(rules_path)
    expected = [
        (amount, fraud)
        for amount, fraud, f in rows_on_test_days(csv)
        if rules.evaluate(f)[0] != "allow"
    ]
    line = result.lines[0]
    assert (line.who, line.rate, line.threshold) == ("rules", "own", None)
    assert line.counts.transfers == len(rows_on_test_days(csv))
    assert line.counts.held == len(expected)
    assert line.counts.fraud_held == sum(fraud for _, fraud in expected)
    assert line.counts.fraud_cents_held == sum(amount for amount, fraud in expected if fraud)


def test_the_model_holds_as_many_as_the_rules_then_1pct(tmp_path: Path, csv: Path) -> None:
    rules_path, model = setup(tmp_path, csv, median_test_amount(csv))
    rules_line, at_rules, at_one = run(csv, rules_path, model, shuffled).lines
    # Distinct scores: no tie at either line, so each holds exactly its count (D55.3).
    assert (at_rules.who, at_rules.rate) == ("model", "rules")
    assert at_rules.counts.held == rules_line.counts.held > 0
    assert (at_one.who, at_one.rate) == ("model", "1%")
    assert at_one.counts.held == capacity(at_one.counts.transfers) > 0
    assert at_rules.threshold is not None
    assert at_one.threshold is not None


def test_a_model_ranking_as_the_rule_does_holds_the_same_transfers(
    tmp_path: Path, csv: Path
) -> None:
    """The rules hold the largest amounts; a model scoring by amount, held to as many, holds the
    same ones, so every count agrees. Amounts here are distinct enough for no tie at the line."""
    amounts = sorted({amount for amount, _, _ in rows_on_test_days(csv)}, reverse=True)
    rules_path, model = setup(tmp_path, csv, amounts[len(amounts) // 10])
    rules_line, at_rules, _ = run(csv, rules_path, model, by_amount).lines
    assert at_rules.counts == rules_line.counts


def test_a_tie_above_every_line_holds_nothing(tmp_path: Path, csv: Path) -> None:
    rules_path, model = setup(tmp_path, csv, median_test_amount(csv))
    _, at_rules, at_one = run(csv, rules_path, model, tied).lines
    for line in (at_rules, at_one):
        assert line.threshold is None
        assert line.counts.held == 0


def test_only_the_test_days_are_scored(tmp_path: Path, csv: Path) -> None:
    seen: list[Rows] = []

    def recording(model: Path, x: Rows) -> tuple[str, Scores]:
        seen.append(x.copy())
        return shuffled(model, x)

    rules_path, model = setup(tmp_path, csv, median_test_amount(csv))
    result = run(csv, rules_path, model, recording)
    expected = np.array([[f[name] for name in FEATURE_NAMES] for _, _, f in rows_on_test_days(csv)])
    assert len(seen) == 1
    assert np.array_equal(seen[0], expected)
    assert result.test_days == (5, 11)  # 12 generated days: the last 7 test (D38)


def test_a_rule_file_from_another_csv_is_refused(tmp_path: Path, csv: Path) -> None:
    rules_file(tmp_path, "0" * 64, 1)
    with pytest.raises(BacktestError, match=r"paysim\.yaml wasn't made from this CSV"):
        run(csv, tmp_path / "paysim.yaml", model_dir(tmp_path / "model", sha256(csv)), shuffled)


@pytest.mark.parametrize("meta", [{"data": {"sha256": "0" * 64}}, {}, []])
def test_a_model_trained_on_another_csv_is_refused(tmp_path: Path, csv: Path, meta: object) -> None:
    rules_path, model = setup(tmp_path, csv, 1)
    (model / "model.json").write_text(json.dumps(meta))
    with pytest.raises(BacktestError, match="model"):
        run(csv, rules_path, model, shuffled)


def test_scores_that_arent_one_finite_number_per_transfer_are_refused(
    tmp_path: Path, csv: Path
) -> None:
    rules_path, model = setup(tmp_path, csv, 1)
    with pytest.raises(BacktestError, match="no finite score"):
        run(csv, rules_path, model, lambda m, x: ("nan", np.full(len(x), np.nan)))
    with pytest.raises(BacktestError, match="no finite score"):
        run(csv, rules_path, model, lambda m, x: ("short", np.zeros(len(x) - 1)))


def test_two_runs_write_the_same_files_and_the_data_reads_back(tmp_path: Path, csv: Path) -> None:
    rules_path, model = setup(tmp_path, csv, median_test_amount(csv))
    written = []
    for run_name in ("a", "b"):
        report, data_path = tmp_path / run_name / "backtest.md", tmp_path / run_name / "b.json"
        result = build(csv, rules_path, model, report, data_path, shuffled)
        written.append((report.read_bytes(), data_path.read_bytes()))
    assert written[0] == written[1]
    document = json.loads(written[0][1])
    assert from_data(document) == result
    assert data(result) == document
    assert "\n".join(comparison(result)) in written[0][0].decode()


def test_the_table_names_each_line_and_its_counts(tmp_path: Path, csv: Path) -> None:
    rules_path, model = setup(tmp_path, csv, median_test_amount(csv))
    result: Result = run(csv, rules_path, model, shuffled)
    lines = comparison(result)
    assert lines[0] == "|  | held | recall | fraud value caught | false-positive rate |"
    assert lines[2].startswith("| PaySim rules (v2), at their own hold rate | ")
    assert lines[3].startswith("| model `stand-in-shuffled`, at the rules' hold rate | ")
    assert lines[4].startswith("| model `stand-in-shuffled`, at 1% | ")
    c = result.lines[0].counts
    assert f"({c.fraud_held} of {c.fraud})" in lines[2]
