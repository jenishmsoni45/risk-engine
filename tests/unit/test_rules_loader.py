"""The rule file loader (design.md 1.4, D27, D46). Phase 4, criterion 1: each invalid file stops the
service at startup with a message naming the problem. This file checks the loader that raises it;
tests/integration/test_rules_startup.py checks that the container stops on one.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from risk.features.compute import FEATURE_NAMES
from risk.rules.ruleset import MAX_RULES, Condition, Rule, RuleSetError, load, parse
from tests.helpers import ROOT

REPO_RULES = ROOT / "risk" / "rules" / "ruleset.yaml"
HEAD = "version: 1\ndefault: allow\non_timeout: hold\n"
GOOD = HEAD + "rules: []\n"
RULE = (
    "rules:\n"
    "  - id: big\n"
    "    action: hold\n"
    "    all:\n"
    "      - feature: amount\n"
    "        op: '>='\n"
    "        value: 100\n"
)
ONE_RULE = HEAD + RULE


def load_text(text: str, tmp_path: Path) -> object:
    path = tmp_path / "ruleset.yaml"
    path.write_text(text)
    return load(path)


def rules_file(rules: list[str]) -> str:
    """A file whose rules are the given YAML flow mappings, one per line."""
    return HEAD + "rules:\n" + "".join(f"  - {text}\n" for text in rules)


def rule(rule_id: str = "r", action: str = "hold", conditions: str | None = None) -> str:
    """One rule as a YAML flow mapping; valid unless an argument breaks it."""
    conditions = conditions or "[{ feature: amount, op: '>=', value: 100 }]"
    return f"{{ id: {rule_id}, action: {action}, all: {conditions} }}"


def condition(feature: str = "amount", op: str = "'>='", value: str = "100") -> str:
    """A rule r with one condition; valid unless an argument breaks it."""
    return rule(conditions=f"[{{ feature: {feature}, op: {op}, value: {value} }}]")


def test_the_repo_rule_set() -> None:
    rules = load(REPO_RULES)
    assert (rules.version, rules.default, rules.on_timeout) == (1, "allow", "hold")
    assert rules.name == "v1"
    assert rules.sha256 == hashlib.sha256(REPO_RULES.read_bytes()).hexdigest()
    assert len(rules.rules) == 6  # D20; tests/unit/test_starter_rules.py checks each one


def test_a_rule_loads_as_written() -> None:
    rules = parse(ONE_RULE.encode(), "f")
    assert rules.rules == (Rule("big", "hold", (Condition("amount", ">=", 100),)),)


def test_an_empty_rule_set_loads() -> None:
    assert parse(GOOD.encode(), "f").rules == ()


@pytest.mark.parametrize("on_timeout", ["allow", "hold", "block"])
def test_each_on_timeout(on_timeout: str) -> None:
    text = GOOD.replace("on_timeout: hold", f"on_timeout: {on_timeout}")
    assert parse(text.encode(), "f").on_timeout == on_timeout


def test_the_hash_is_of_the_file_bytes() -> None:
    one = parse(GOOD.encode(), "f").sha256
    assert one == hashlib.sha256(GOOD.encode()).hexdigest()
    # The same rule set spelled differently is a different file, so a different hash.
    assert parse(("# a comment\n" + GOOD).encode(), "f").sha256 != one


def test_every_operator_feature_and_both_actions_load() -> None:
    ops = (">", ">=", "<", "<=", "==", "!=")
    rules = [
        rule(
            f"r{n}",
            ("hold", "block")[n % 2],
            f"[{{ feature: {feature}, op: '{ops[n % len(ops)]}', value: {n - 15} }}]",
        )
        for n, feature in enumerate(FEATURE_NAMES)
    ]
    loaded = parse(rules_file(rules).encode(), "f").rules
    assert [r.conditions[0].feature for r in loaded] == list(FEATURE_NAMES)
    assert {r.conditions[0].op for r in loaded} == set(ops)
    assert {r.action for r in loaded} == {"hold", "block"}
    # Values are any integer, negative ones included.
    assert loaded[0].conditions[0].value == -15


def test_the_limits_themselves_load() -> None:
    """At most 100 rules (D40x) and ids of up to 64 characters of a-z, 0-9 and _."""
    longest = [f"r{n:03d}_".ljust(64, "x") for n in range(MAX_RULES)]
    loaded = parse(rules_file([rule(f"'{i}'") for i in longest]).encode(), "f").rules
    assert [r.id for r in loaded] == longest
    assert all(len(r.id) == 64 for r in loaded)
    shortest = ["_", "0", "a"]
    loaded = parse(rules_file([rule(f"'{i}'") for i in shortest]).encode(), "f").rules
    assert [r.id for r in loaded] == shortest


# Each invalid file, the text its message must hold, and where the problem is. Criterion 1's
# classes: not YAML, a missing or unknown key, a repeated or malformed id, an unknown feature, a
# bad operator, a non-integer value, an allow action, an empty all, more than 100 rules; plus a
# repeated key (D46) and the top-level values Phase 2 already checked.
BROKEN: dict[str, tuple[str, str]] = {
    # Not YAML, or not the shape of a rule file.
    "not YAML": ("version: [1\n", "not YAML"),
    "an unquoted > op, which YAML reads as a block scalar": (
        HEAD + "rules:\n  - id: r\n    action: hold\n    all:\n      - op: >=\n",
        "not YAML",
    ),
    "a Python tag": (GOOD + "x: !!python/object:os.system {}\n", "not YAML"),
    "an empty file": ("", "the file must be a mapping"),
    "not a mapping": ("- 1\n- 2\n", "the file must be a mapping"),
    # Missing and unknown keys, at every level.
    "an unknown top-level key": (GOOD + "extra: 1\n", "the file has unknown keys ['extra']"),
    "version missing": (GOOD.replace("version: 1\n", ""), "the file is missing keys ['version']"),
    "default missing": (GOOD.replace("default: allow\n", ""), "missing keys ['default']"),
    "on_timeout missing": (GOOD.replace("on_timeout: hold\n", ""), "missing keys ['on_timeout']"),
    "rules missing": (HEAD, "the file is missing keys ['rules']"),
    "a rule not a mapping": (rules_file(["velocity"]), "rules[0] must be a mapping"),
    "a rule with an unknown key": (
        rules_file(["{ id: r, action: hold, all: [], when: now }"]),
        "rules[0] has unknown keys ['when']",
    ),
    "a rule without an id": (
        rules_file(["{ action: hold, all: [{ feature: amount, op: '>', value: 1 }] }"]),
        "rules[0] is missing keys ['id']",
    ),
    "a rule without an action": (
        rules_file(["{ id: r, all: [{ feature: amount, op: '>', value: 1 }] }"]),
        "rules[0] is missing keys ['action']",
    ),
    "a rule without all": (rules_file(["{ id: r, action: hold }"]), "missing keys ['all']"),
    "a condition not a mapping": (
        rules_file([rule(conditions="[amount]")]),
        "rules[0] (r).all[0] must be a mapping",
    ),
    "a condition with an unknown key": (
        rules_file([rule(conditions="[{ feature: amount, op: '>', value: 1, window: 1m }]")]),
        "rules[0] (r).all[0] has unknown keys ['window']",
    ),
    "a condition without a value": (
        rules_file([rule(conditions="[{ feature: amount, op: '>' }]")]),
        "rules[0] (r).all[0] is missing keys ['value']",
    ),
    "a condition without a feature": (
        rules_file([rule(conditions="[{ op: '>', value: 1 }]")]),
        "is missing keys ['feature']",
    ),
    "a condition without an op": (
        rules_file([rule(conditions="[{ feature: amount, value: 1 }]")]),
        "is missing keys ['op']",
    ),
    # A key given twice in one mapping (D46).
    "a repeated top-level key": (GOOD + "version: 2\n", "the key 'version' is repeated at line 5"),
    "a repeated rule key": (
        HEAD + RULE.replace("    action: hold\n", "    action: hold\n    action: block\n"),
        "the key 'action' is repeated at line 7",
    ),
    "a repeated condition key": (
        ONE_RULE + "        value: 50\n",
        "the key 'value' is repeated at line 11",
    ),
    # Repeated and malformed ids.
    "a repeated id": (
        rules_file([rule("velocity"), rule("other"), rule("velocity")]),
        "rules[2]: id 'velocity' is already used by rules[0]",
    ),
    "an uppercase id": (rules_file([rule("Velocity")]), "rules[0]: id must be"),
    "an id with a hyphen": (rules_file([rule("velocity-1m")]), "rules[0]: id must be"),
    "an id with a space": (rules_file([rule("'velocity 1m'")]), "id must be"),
    "an empty id": (rules_file([rule("''")]), "id must be"),
    "an id of 65 characters": (rules_file([rule("r" * 65)]), "id must be"),
    "an id YAML reads as a number": (rules_file([rule("123")]), "got 123"),
    "an id YAML reads as a bool": (rules_file([rule("yes")]), "got True"),
    "an id YAML reads as null": (rules_file([rule("null")]), "got None"),
    # Unknown features.
    "an unknown feature": (rules_file([condition(feature="out_count_2m")]), "'out_count_2m'"),
    "a feature in the wrong case": (rules_file([condition(feature="Amount")]), "unknown feature"),
    "a feature that isn't text": (rules_file([condition(feature="1")]), "unknown feature 1"),
    # Bad operators.
    "op =>": (rules_file([condition(op="'=>'")]), "op must be one of"),
    "op =": (rules_file([condition(op="'='")]), "rules[0] (r).all[0]: op must be one of"),
    "op <>": (rules_file([condition(op="'<>'")]), "got '<>'"),
    "op as a word": (rules_file([condition(op="gte")]), "got 'gte'"),
    # Values that aren't integers.
    "a float value": (rules_file([condition(value="100.0")]), "value must be an integer"),
    "a string value": (rules_file([condition(value="'100'")]), "got '100'"),
    "a bool value": (rules_file([condition(value="true")]), "got True"),
    "a null value": (rules_file([condition(value="null")]), "got None"),
    "a list value": (rules_file([condition(value="[100]")]), "got [100]"),
    # Actions.
    "an allow action": (rules_file([rule(action="allow")]), "rules[0] (r): action must be one of"),
    "an unknown action": (rules_file([rule(action="review")]), "got 'review'"),
    "an uppercase action": (rules_file([rule(action="HOLD")]), "got 'HOLD'"),
    # all must be a non-empty list.
    "an empty all": (rules_file([rule(conditions="[]")]), "rules[0] (r): all must be a non-empty"),
    "all a mapping": (
        rules_file([rule(conditions="{ feature: amount, op: '>', value: 1 }")]),
        "all must be a non-empty list",
    ),
    # More than 100 rules (D40x).
    "101 rules": (
        rules_file([rule(f"r{n}") for n in range(MAX_RULES + 1)]),
        "rules holds 101 rules, more than 100",
    ),
    # The top-level values.
    "version zero": (GOOD.replace("version: 1", "version: 0"), "version must be"),
    "version a float": (GOOD.replace("version: 1", "version: 1.5"), "version must be"),
    "version a string": (GOOD.replace("version: 1", "version: '1'"), "version must be"),
    "version true": (GOOD.replace("version: 1", "version: true"), "version must be"),
    "default hold": (GOOD.replace("default: allow", "default: hold"), "default must be allow"),
    "on_timeout unknown": (GOOD.replace("on_timeout: hold", "on_timeout: deny"), "on_timeout"),
    # YAML 1.1 reads a bare no as false.
    "on_timeout no": (GOOD.replace("on_timeout: hold", "on_timeout: no"), "got False"),
    "on_timeout uppercase": (GOOD.replace("on_timeout: hold", "on_timeout: HOLD"), "on_timeout"),
    "rules not a list": (GOOD.replace("rules: []", "rules: {}"), "rules must be a list"),
    "rules null": (GOOD.replace("rules: []", "rules:"), "rules must be a list"),
}


@pytest.mark.parametrize(("text", "named"), BROKEN.values(), ids=BROKEN.keys())
def test_a_broken_file_names_its_problem(tmp_path: Path, text: str, named: str) -> None:
    with pytest.raises(RuleSetError) as raised:
        load_text(text, tmp_path)
    message = str(raised.value)
    # The message starts with the file, then names the problem.
    assert message.startswith(f"{tmp_path / 'ruleset.yaml'}: ")
    assert named in message


def test_a_problem_late_in_the_file_names_its_rule(tmp_path: Path) -> None:
    rules = [rule(f"r{n}") for n in range(5)] + [
        condition(feature="nope").replace("id: r", "id: z")
    ]
    with pytest.raises(RuleSetError, match=r"rules\[5\] \(z\)\.all\[0\]: unknown feature 'nope'"):
        load_text(rules_file(rules), tmp_path)


def test_a_missing_file(tmp_path: Path) -> None:
    with pytest.raises(RuleSetError, match="can't read"):
        load(tmp_path / "nope.yaml")
