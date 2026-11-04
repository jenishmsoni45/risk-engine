"""The PaySim rule set (design.md section 8, D54; Phase 6 criterion 10), on generated data: the file
is D20's rules with only the cent thresholds multiplied, the service's loader accepts it, its
factor is the smallest that stops at most 1% of the training days' transfers, and what no factor
can bring to 1% is refused. The committed file is D20's rules times the factor it names.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, replace
from pathlib import Path

import pytest

from ml.generate import generate, write_csv
from ml.offline import OfflineRow, features
from ml.paysim import load
from ml.paysim_rules import CENT_FEATURES, RULES_OUT, PaySimRulesError, build, render
from ml.profile import RULES, sha256
from ml.split import day_of, part_of
from risk.features.compute import FEATURE_NAMES
from risk.rules import ruleset

DAYS = 20  # days 0 to 19, so days 0 to 10 train (D38)

ALWAYS = b"""version: 1
default: allow
on_timeout: hold
rules:
  - id: always
    action: hold
    all:
      - { feature: attempt_count_24h, op: ">=", value: 0 }
"""

BELOW = b"""version: 1
default: allow
on_timeout: hold
rules:
  - id: small_transfer
    action: hold
    all:
      - { feature: amount, op: "<", value: 100 }
"""


@dataclass(frozen=True)
class Built:
    factor: int
    rules: Path
    report: Path


@pytest.fixture(scope="module")
def csv_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    path = tmp_path_factory.mktemp("paysim") / "generated.csv"
    write_csv(path, generate(seed=6, steps=24 * DAYS))
    return path


@pytest.fixture(scope="module")
def built(csv_path: Path, tmp_path_factory: pytest.TempPathFactory) -> Built:
    out = tmp_path_factory.mktemp("built")
    rules, report = out / "paysim.yaml", out / "paysim_rules.md"
    return Built(build(csv_path, rules, report), rules, report)


@pytest.fixture(scope="module")
def training(csv_path: Path) -> list[OfflineRow]:
    rows = list(features(load(csv_path)))
    last_day = day_of(rows[-1].transfer.time_ms)
    return [row for row in rows if part_of(day_of(row.transfer.time_ms), last_day) == "train"]


def times(rules: ruleset.RuleSet, factor: int) -> ruleset.RuleSet:
    """The rules with every cent threshold times factor, built without ml.paysim_rules."""
    return replace(
        rules,
        rules=tuple(
            replace(
                rule,
                conditions=tuple(
                    replace(c, value=c.value * factor) if c.feature in CENT_FEATURES else c
                    for c in rule.conditions
                ),
            )
            for rule in rules.rules
        ),
    )


def stops(rules: ruleset.RuleSet, rows: list[OfflineRow]) -> int:
    return sum(rules.evaluate(row.features)[0] != "allow" for row in rows)


def test_the_cent_features_are_the_amount_and_every_sum() -> None:
    sums = {name for name in FEATURE_NAMES if re.fullmatch(r"[a-z_]+_sum_(1m|1h|24h)", name)}
    assert len(sums) == 15
    assert sums | {"amount"} == CENT_FEATURES


def test_the_factor_is_the_smallest_that_stops_at_most_1_percent(
    built: Built, training: list[OfflineRow]
) -> None:
    v1 = ruleset.load(RULES)
    at_most = len(training) // 100
    assert built.factor > 1, "D20's thresholds should stop more than 1% of the generated data"
    assert stops(times(v1, built.factor), training) <= at_most
    assert stops(times(v1, built.factor - 1), training) > at_most
    assert stops(ruleset.load(built.rules), training) <= at_most


def test_the_file_is_d20s_rules_with_only_the_cent_thresholds_multiplied(built: Built) -> None:
    v1, paysim = ruleset.load(RULES), ruleset.load(built.rules)
    assert (paysim.version, paysim.default, paysim.on_timeout) == (2, "allow", v1.on_timeout)
    assert paysim.rules == times(v1, built.factor).rules
    large = next(rule for rule in paysim.rules if rule.id == "large_transfer")
    assert large.conditions[0].value == 1_000_000 * built.factor


def test_the_report_names_the_data_the_file_and_the_training_days(
    built: Built, csv_path: Path, training: list[OfflineRow]
) -> None:
    text = built.report.read_text()
    paysim = ruleset.load(built.rules)
    fraud = sum(row.transfer.is_fraud for row in training)
    assert f"- Data: `generated.csv`, SHA-256 `{sha256(csv_path)}`" in text
    assert f"- Rule file: `paysim.yaml`, v2, SHA-256 `{paysim.sha256}`" in text
    assert f"- Days 0 to {DAYS - 1}; the training days are 0 to 10 (D38)" in text
    assert f"with {len(training)} transfers, of which fraud {fraud}" in text
    assert f"- Factor: {built.factor}. " in text
    for decision in ("allow", "hold", "block"):
        count = sum(paysim.evaluate(row.features)[0] == decision for row in training)
        assert f"| {decision} | {count} |" in text


def test_two_runs_write_the_same_files(built: Built, csv_path: Path, tmp_path: Path) -> None:
    rules, report = tmp_path / "paysim.yaml", tmp_path / "paysim_rules.md"
    assert build(csv_path, rules, report) == built.factor
    assert rules.read_bytes() == built.rules.read_bytes()
    assert report.read_bytes() == built.report.read_bytes()


def test_rules_no_factor_brings_to_1_percent_are_refused(csv_path: Path, tmp_path: Path) -> None:
    source = tmp_path / "always.yaml"
    source.write_bytes(ALWAYS)
    out = tmp_path / "out.yaml"
    with pytest.raises(PaySimRulesError, match="more than 1%"):
        build(csv_path, out, tmp_path / "report.md", source)
    assert not out.exists()


def test_a_cent_condition_a_factor_cant_scale_is_refused() -> None:
    with pytest.raises(PaySimRulesError, match="must be > or >= a value of at least 0"):
        render(ruleset.parse(BELOW, "below.yaml"), 2)


def test_data_too_short_to_leave_a_training_day_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "short.csv"
    write_csv(path, generate(seed=6, steps=24 * 9))
    with pytest.raises(PaySimRulesError, match="at least 10 days"):
        build(path, tmp_path / "out.yaml", tmp_path / "report.md")


def test_the_committed_file_is_d20s_rules_times_the_factor_it_names() -> None:
    text = RULES_OUT.read_text()
    found = re.search(r"^# multiplied by (\d+):", text, re.MULTILINE)
    assert found, "the header names the factor"
    paysim = ruleset.load(RULES_OUT)
    assert paysim.version == 2
    assert paysim.rules == times(ruleset.load(RULES), int(found.group(1))).rules
