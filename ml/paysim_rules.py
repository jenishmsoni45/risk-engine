"""The PaySim rule set (design.md section 8, D54): D20's rules with every threshold on a cent
feature multiplied by one integer factor, the smallest at which the set holds or blocks at most 1%
of the training days' transfers (D34, D38).

PaySim's currency is unnamed, so one factor changes the scale of the money and nothing else: the
count thresholds, ids, actions and order stay as in `risk/rules/ruleset.yaml`. Every candidate is
written as YAML, read back by the service's own loader and evaluated by the service's own
function, so the search, the file and the rules engine can't disagree. Only the backtest reads the
file (D40q); the service never loads it.

A larger factor raises every cent threshold, and every cent condition must be `>` or `>=` a value
of at least 0, so a larger factor never stops a transfer that a smaller one allowed. The search
relies on that: it doubles the factor until few enough transfers are stopped, then bisects, each
time checking only the transfers that the last factor found too small stopped.

The test days are never evaluated here, so they stay unseen until the backtest. Every number in
the report comes from this script (D29). Run `make paysim-rules`.
"""

from __future__ import annotations

import argparse
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

from ml.offline import features
from ml.paysim import PaySimError, load
from ml.profile import RULES, sha256, share, table
from ml.split import day_of, part_of
from risk.features.compute import FEATURE_NAMES
from risk.rules import ruleset

ROOT = Path(__file__).resolve().parent.parent
RULES_OUT = ROOT / "ml" / "rules" / "paysim.yaml"
REPORT = ROOT / "ml" / "reports" / "paysim_rules.md"
VERSION = 2  # D54
HOLD_SHARE = 100  # at most one training transfer in 100 held or blocked (D34)
CENT_FEATURES = frozenset(name for name in FEATURE_NAMES if name == "amount" or "_sum_" in name)
SCALABLE_OPS = (">", ">=")

Values = tuple[int, ...]  # one transfer's values of the features the rules read


class PaySimRulesError(Exception):
    """No PaySim rule set can be written; the message says why."""


@dataclass
class Sample:
    """The training days' transfers, reduced to what the rules read."""

    names: tuple[str, ...]  # the features the source rules read, in the order of each Values
    values: list[Values]
    fraud: list[bool]
    last_day: int
    last_training_day: int


@dataclass(frozen=True)
class Choice:
    factor: int
    at_most: int  # training transfers the set may stop
    stopped_below: int | None  # what the factor less 1 stops; None when the factor is 1


def render(source: ruleset.RuleSet, factor: int, header: Sequence[str] = ()) -> str:
    """The source rules as a rule file, version 2, with every cent threshold times factor."""
    lines = [f"# {line}".rstrip() for line in header]
    lines += [f"version: {VERSION}", "default: allow", f"on_timeout: {source.on_timeout}"]
    lines.append("rules:" if source.rules else "rules: []")
    for rule in source.rules:
        lines += [f"  - id: {rule.id}", f"    action: {rule.action}", "    all:"]
        for condition in rule.conditions:
            value = condition.value
            if condition.feature in CENT_FEATURES:
                if condition.op not in SCALABLE_OPS or value < 0:
                    raise PaySimRulesError(
                        f"{rule.id}: a cent condition must be > or >= a value of at least 0 to "
                        f"scale, got {condition.feature} {condition.op} {value}"
                    )
                value *= factor
            lines.append(
                f'      - {{ feature: {condition.feature}, op: "{condition.op}", value: {value} }}'
            )
    return "\n".join(lines) + "\n"


def scaled(source: ruleset.RuleSet, factor: int) -> ruleset.RuleSet:
    return ruleset.parse(render(source, factor).encode(), f"{source.name} times {factor}")


def training_sample(path: Path, source: ruleset.RuleSet) -> Sample:
    names = tuple(sorted({c.feature for rule in source.rules for c in rule.conditions}))
    days: list[int] = []
    values: list[Values] = []
    fraud: list[bool] = []
    for row in features(load(path)):
        days.append(day_of(row.transfer.time_ms))
        values.append(tuple(row.features[name] for name in names))
        fraud.append(row.transfer.is_fraud)
    if not days:
        raise PaySimRulesError(f"{path.name} holds no transfer to load")
    last_day = days[-1]  # features() yields in time order, so days never decrease
    try:
        training = sum(part_of(day, last_day) == "train" for day in days)
    except ValueError as exc:
        raise PaySimRulesError(str(exc)) from exc
    if training == 0:
        raise PaySimRulesError(f"{path.name} holds no transfer on a training day (D38)")
    # The training days come first, so their transfers are the first `training`.
    del values[training:], fraud[training:]
    last_training_day = max(d for d in range(last_day + 1) if part_of(d, last_day) == "train")
    return Sample(names, values, fraud, last_day, last_training_day)


def stopped(
    rules: ruleset.RuleSet, names: tuple[str, ...], among: Iterable[Values]
) -> list[Values]:
    """The transfers the rules hold or block."""
    return [v for v in among if rules.evaluate(dict(zip(names, v, strict=True)))[0] != "allow"]


def search(source: ruleset.RuleSet, names: tuple[str, ...], values: Sequence[Values]) -> Choice:
    """The smallest integer factor at which the scaled rules stop at most 1% of values."""
    at_most = len(values) // HOLD_SHARE
    # Above every cent value in the sample, no cent threshold of at least 1 can be reached, so a
    # larger factor changes nothing.
    ceiling = max(
        (v for row in values for name, v in zip(names, row, strict=True) if name in CENT_FEATURES),
        default=0,
    )
    low, candidates = 0, list(values)  # low stops too many; candidates holds all it stops
    factor = 1
    while len(hit := stopped(scaled(source, factor), names, candidates)) > at_most:
        if factor > ceiling:
            raise PaySimRulesError(
                f"with every cent threshold above every amount and sum, the rules still stop "
                f"{len(hit)} of the {len(values)} training transfers, more than 1%"
            )
        low, candidates = factor, hit
        factor *= 2
    high = factor
    while high - low > 1:
        middle = (low + high) // 2
        hit = stopped(scaled(source, middle), names, candidates)
        if len(hit) <= at_most:
            high = middle
        else:
            low, candidates = middle, hit
    return Choice(high, at_most, len(candidates) if low else None)


def _shown(path: Path) -> str:
    return str(path.relative_to(ROOT)) if path.is_relative_to(ROOT) else path.name


def report(
    path: Path,
    digest: str,
    rules_out: Path,
    source: ruleset.RuleSet,
    rules: ruleset.RuleSet,
    sample: Sample,
    choice: Choice,
) -> str:
    decisions: Counter[tuple[str, bool]] = Counter()
    fired: Counter[tuple[str, bool]] = Counter()
    for values, is_fraud in zip(sample.values, sample.fraud, strict=True):
        decision, ids = rules.evaluate(dict(zip(sample.names, values, strict=True)))
        decisions[decision, is_fraud] += 1
        for rule_id in ids:
            fired[rule_id, is_fraud] += 1
    transfers = len(sample.values)
    fraud = sum(sample.fraud)
    legit = transfers - fraud
    below = (
        f"at {choice.factor - 1} it would stop {choice.stopped_below}"
        if choice.stopped_below is not None
        else "1 is the smallest factor there is"
    )
    lines = [
        "# The PaySim rule set",
        "",
        "Written by `make paysim-rules` (`python -m ml.paysim_rules`) together with the rule file; "
        "every number below comes from that script (D29). D54 sets what it does: D20's rules with "
        "every threshold on a cent feature multiplied by the smallest integer factor at which the "
        "set holds or blocks at most 1% of the training days' transfers. Only the backtest reads "
        "the file (D40q).",
        "",
        f"- Data: `{path.name}`, SHA-256 `{digest}`",
        f"- Rule file: `{_shown(rules_out)}`, {rules.name}, SHA-256 `{rules.sha256}`",
        f"- Days 0 to {sample.last_day}; the training days are 0 to {sample.last_training_day} "
        f"(D38), with {transfers} transfers, of which fraud {fraud}",
        f"- Factor: {choice.factor}. The set may stop at most {choice.at_most} training transfers "
        f"(1%); {below}.",
        "",
        "## Thresholds",
        "",
        *table(
            ("rule", "action", "condition", "v1 (D20)", f"PaySim ({rules.name})"),
            (
                (rule.id, rule.action, f"`{old.feature} {old.op}`", old.value, new.value)
                for rule, new_rule in zip(source.rules, rules.rules, strict=True)
                for old, new in zip(rule.conditions, new_rule.conditions, strict=True)
            ),
        ),
        "",
        "## On the training days",
        "",
        "The test days aren't evaluated here, so they stay unseen until the backtest (D38).",
        "",
        *table(
            ("decision", "transfers", "share", "of fraud", "of not fraud"),
            (
                (
                    decision,
                    decisions[decision, True] + decisions[decision, False],
                    share(decisions[decision, True] + decisions[decision, False], transfers),
                    share(decisions[decision, True], fraud),
                    share(decisions[decision, False], legit),
                )
                for decision in ("allow", "hold", "block")
            ),
        ),
        "",
        *table(
            ("rule", "fired on", "share", "of fraud", "of not fraud"),
            (
                (
                    rule.id,
                    fired[rule.id, True] + fired[rule.id, False],
                    share(fired[rule.id, True] + fired[rule.id, False], transfers),
                    share(fired[rule.id, True], fraud),
                    share(fired[rule.id, False], legit),
                )
                for rule in rules.rules
            ),
        ),
        "",
    ]
    return "\n".join(lines)


def build(
    path: Path,
    rules_out: Path = RULES_OUT,
    report_out: Path = REPORT,
    source_path: Path = RULES,
) -> int:
    """Writes the rule file and its report; returns the factor."""
    source = ruleset.load(source_path)
    digest = sha256(path)
    try:
        sample = training_sample(path, source)
    except PaySimError as exc:
        raise PaySimRulesError(f"the loader refuses {path.name}: {exc}") from exc
    choice = search(source, sample.names, sample.values)
    header = [
        "The PaySim rule set (D54), written by `make paysim-rules`; don't edit it by hand.",
        f"D20's rules from {_shown(source_path)}, with every threshold on a cent feature",
        f"multiplied by {choice.factor}: the smallest integer factor at which the set holds or",
        "blocks at most 1% of the training days' transfers (D38) in",
        f"{path.name} (SHA-256 {digest}).",
        "Only the backtest reads this file (D40q); the service loads risk/rules/ruleset.yaml.",
    ]
    data = render(source, choice.factor, header).encode()
    rules = ruleset.parse(data, str(rules_out))
    rules_out.parent.mkdir(parents=True, exist_ok=True)
    rules_out.write_bytes(data)
    report_out.parent.mkdir(parents=True, exist_ok=True)
    report_out.write_text(report(path, digest, rules_out, source, rules, sample, choice))
    return choice.factor


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--csv", type=Path, required=True, help="a PaySim CSV")
    parser.add_argument("--rules-out", type=Path, default=RULES_OUT)
    parser.add_argument("--report-out", type=Path, default=REPORT)
    args = parser.parse_args(argv)
    if not args.csv.is_file():
        print(f"{args.csv} doesn't exist: put the PaySim CSV in data/ (CLAUDE.md)")
        return 2
    try:
        factor = build(args.csv, args.rules_out, args.report_out)
    except PaySimRulesError as exc:
        print(f"no PaySim rule set written: {exc}")
        return 1
    print(f"factor {factor}: wrote {args.rules_out} and {args.report_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
