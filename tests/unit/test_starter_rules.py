"""The starter rule set (D20, design.md 1.4). Phase 4, criterion 3: for each starter rule, features
one below the threshold don't fire it and features at the threshold do; a hold and a block firing
together give block with both listed.

D20's thresholds are written out here rather than read from the file, so a changed threshold
fails this test until D20 changes with it.
"""

from __future__ import annotations

import pytest

from risk.features.compute import FEATURE_NAMES
from risk.rules.ruleset import Condition, Rule, load
from tests.helpers import ROOT

STARTER_RULES = load(ROOT / "risk" / "rules" / "ruleset.yaml")

# D20, in file order: the id, the action, and each condition's feature and threshold (all >=).
D20: tuple[tuple[str, str, dict[str, int]], ...] = (
    ("velocity_1m", "hold", {"out_count_1m": 5}),
    ("attempt_burst_1m", "block", {"attempt_count_1m": 10}),
    ("daily_outflow_24h", "hold", {"out_sum_24h": 2_500_000}),
    ("large_transfer", "hold", {"amount": 1_000_000}),
    ("dest_fan_in_1h", "hold", {"dest_in_count_1h": 20}),
    ("pass_through_1h", "hold", {"in_sum_1h": 200_000, "amount": 200_000}),
)


def features(**values: int) -> dict[str, int]:
    """No activity, and the smallest amount a request may carry."""
    return dict.fromkeys(FEATURE_NAMES, 0) | {"amount": 1} | values


def test_the_file_is_d20() -> None:
    assert (STARTER_RULES.version, STARTER_RULES.default, STARTER_RULES.on_timeout) == (
        1,
        "allow",
        "hold",
    )
    assert STARTER_RULES.rules == tuple(
        Rule(rule_id, action, tuple(Condition(f, ">=", v) for f, v in thresholds.items()))
        for rule_id, action, thresholds in D20
    )


def test_nothing_fires_without_activity() -> None:
    assert STARTER_RULES.evaluate(features()) == ("allow", [])


@pytest.mark.parametrize(("rule_id", "action", "thresholds"), D20, ids=[r[0] for r in D20])
def test_at_the_threshold_the_rule_fires(
    rule_id: str, action: str, thresholds: dict[str, int]
) -> None:
    assert STARTER_RULES.evaluate(features(**thresholds)) == (action, [rule_id])
    # Further past it, still that rule alone.
    far = {name: value * 3 for name, value in thresholds.items()}
    assert STARTER_RULES.evaluate(features(**far)) == (action, [rule_id])


# Each condition of each rule taken one below its threshold, the rule's other conditions at theirs.
ONE_BELOW = [
    pytest.param(thresholds | {name: value - 1}, id=f"{rule_id}-{name}")
    for rule_id, _, thresholds in D20
    for name, value in thresholds.items()
]


@pytest.mark.parametrize("values", ONE_BELOW)
def test_one_below_the_threshold_the_rule_doesnt_fire(values: dict[str, int]) -> None:
    assert STARTER_RULES.evaluate(features(**values)) == ("allow", [])


def test_a_hold_and_a_block_together_give_block_with_both() -> None:
    together = features(out_count_1m=5, attempt_count_1m=10)
    assert STARTER_RULES.evaluate(together) == ("block", ["velocity_1m", "attempt_burst_1m"])


def test_every_starter_rule_at_once() -> None:
    every = features(**{name: value for _, _, t in D20 for name, value in t.items()})
    # amount takes pass_through_1h's 200,000 last; large_transfer needs 1,000,000.
    every["amount"] = 1_000_000
    assert STARTER_RULES.evaluate(every) == ("block", [rule_id for rule_id, _, _ in D20])
