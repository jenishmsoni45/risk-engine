"""Loading the rule file (design.md 1.4) and evaluating it (design.md section 5).

The file is read once at startup (D27). Anything wrong with it raises RuleSetError with a message
naming the problem and where it is, and the service doesn't start. The loaded rule set is
immutable, and evaluation is pure: the same features always give the same decision and fired
list, which I8 and the backtest rely on.
"""

from __future__ import annotations

import hashlib
import operator
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from risk.features.compute import FEATURE_NAMES
from risk.settings import DECISIONS

KEYS = ("version", "default", "on_timeout", "rules")
RULE_KEYS = ("id", "action", "all")
CONDITION_KEYS = ("feature", "op", "value")
ACTIONS = ("hold", "block")
RULE_ID = re.compile(r"[a-z0-9_]{1,64}")
# Keeps every 200 body far under tally's 16 KiB cap, whatever fires (D40x).
MAX_RULES = 100

OPERATORS: Mapping[str, Callable[[int, int], bool]] = {
    ">": operator.gt,
    ">=": operator.ge,
    "<": operator.lt,
    "<=": operator.le,
    "==": operator.eq,
    "!=": operator.ne,
}
# The strictest fired action wins: block over hold over allow (D21).
STRICTNESS = {"allow": 0, "hold": 1, "block": 2}


class RuleSetError(Exception):
    """The rule file can't be used; the message names the problem."""


@dataclass(frozen=True)
class Condition:
    feature: str
    op: str
    value: int

    def holds(self, features: Mapping[str, int]) -> bool:
        return OPERATORS[self.op](features[self.feature], self.value)


@dataclass(frozen=True)
class Rule:
    id: str
    action: str
    conditions: tuple[Condition, ...]  # the file's `all`: every one must hold

    def fires(self, features: Mapping[str, int]) -> bool:
        return all(condition.holds(features) for condition in self.conditions)


@dataclass(frozen=True)
class RuleSet:
    version: int
    default: str
    on_timeout: str
    sha256: str  # of the file's bytes, stored with every decision (design.md 1.4)
    rules: tuple[Rule, ...] = ()

    @property
    def name(self) -> str:
        """`ruleSet` in responses (design.md 1.4)."""
        return f"v{self.version}"

    def evaluate(self, features: Mapping[str, int]) -> tuple[str, list[str]]:
        """design.md section 5: every rule whose conditions all hold fires; the decision is the
        strictest fired action, else `default`; fired lists the fired ids in file order."""
        fired = [rule for rule in self.rules if rule.fires(features)]
        if not fired:
            return self.default, []
        decision = max((rule.action for rule in fired), key=STRICTNESS.__getitem__)
        return decision, [rule.id for rule in fired]


# --- Reading YAML ----------------------------------------------------------------------------


class _RepeatedKeyError(Exception):
    def __init__(self, key: object, line: int) -> None:
        super().__init__(f"the key {key!r} is repeated at line {line}")


class _Loader(yaml.SafeLoader):  # type: ignore[misc]  # PyYAML has no type hints
    """PyYAML's safe loader, except that a key repeated in one mapping is an error (D46): YAML
    requires unique keys, but PyYAML keeps the last value without a word."""

    def construct_mapping(self, node: Any, deep: bool = False) -> Any:
        if not isinstance(node, yaml.MappingNode):
            return super().construct_mapping(node, deep=deep)  # PyYAML refuses it
        seen: set[object] = set()
        for key_node, _ in node.value:
            if key_node.tag == "tag:yaml.org,2002:merge":
                continue  # a `<<` merge, which PyYAML resolves itself
            key = self.construct_object(key_node, deep=deep)
            try:
                repeated = key in seen
            except TypeError:
                continue  # unhashable: PyYAML refuses it below
            if repeated:
                raise _RepeatedKeyError(key, key_node.start_mark.line + 1)
            seen.add(key)
        return super().construct_mapping(node, deep=deep)


def _is_int(value: object) -> bool:
    # YAML reads `true` as a bool, which Python counts as an int.
    return isinstance(value, int) and not isinstance(value, bool)


def _check_keys(where: str, mapping: object, keys: tuple[str, ...]) -> dict[Any, Any]:
    if not isinstance(mapping, dict):
        raise RuleSetError(f"{where} must be a mapping with the keys {list(keys)}, got {mapping!r}")
    unknown = sorted(str(key) for key in mapping if key not in keys)
    if unknown:
        raise RuleSetError(f"{where} has unknown keys {unknown}")
    missing = [key for key in keys if key not in mapping]
    if missing:
        raise RuleSetError(f"{where} is missing keys {missing}")
    return mapping


def _condition(where: str, raw: object) -> Condition:
    condition = _check_keys(where, raw, CONDITION_KEYS)
    feature, op, value = (condition[key] for key in CONDITION_KEYS)
    if not isinstance(feature, str) or feature not in FEATURE_NAMES:
        raise RuleSetError(f"{where}: unknown feature {feature!r} (design.md 1.5 lists the 31)")
    if not isinstance(op, str) or op not in OPERATORS:
        raise RuleSetError(f"{where}: op must be one of {list(OPERATORS)}, got {op!r}")
    if not _is_int(value):
        raise RuleSetError(f"{where}: value must be an integer, got {value!r}")
    return Condition(feature, op, value)


def _rule(where: str, raw: object, ids: dict[str, str]) -> Rule:
    rule = _check_keys(where, raw, RULE_KEYS)
    rule_id = rule["id"]
    if not isinstance(rule_id, str) or not RULE_ID.fullmatch(rule_id):
        raise RuleSetError(
            f"{where}: id must be 1 to 64 of a-z, 0-9 and _, as quoted text if YAML would read it "
            f"as something else, got {rule_id!r}"
        )
    if rule_id in ids:
        raise RuleSetError(f"{where}: id {rule_id!r} is already used by {ids[rule_id]}")
    ids[rule_id] = where
    where = f"{where} ({rule_id})"
    action = rule["action"]
    if not isinstance(action, str) or action not in ACTIONS:
        raise RuleSetError(f"{where}: action must be one of {list(ACTIONS)}, got {action!r}")
    conditions = rule["all"]
    if not isinstance(conditions, list) or not conditions:
        raise RuleSetError(f"{where}: all must be a non-empty list, got {conditions!r}")
    return Rule(
        id=rule_id,
        action=action,
        conditions=tuple(_condition(f"{where}.all[{n}]", c) for n, c in enumerate(conditions)),
    )


def parse(data: bytes, source: str) -> RuleSet:
    """The rule set in data, or RuleSetError naming the problem; source names the file."""
    try:
        document: Any = yaml.load(data, Loader=_Loader)  # noqa: S506 - a SafeLoader subclass
    except _RepeatedKeyError as exc:
        raise RuleSetError(f"{source}: {exc}") from None
    except yaml.YAMLError as exc:
        raise RuleSetError(f"{source}: not YAML: {exc}") from exc

    try:
        top = _check_keys("the file", document, KEYS)
        version = top["version"]
        if not _is_int(version) or version < 1:
            raise RuleSetError(f"version must be an integer of at least 1, got {version!r}")
        if top["default"] != "allow":
            raise RuleSetError(f"default must be allow, got {top['default']!r}")
        on_timeout = top["on_timeout"]
        if not isinstance(on_timeout, str) or on_timeout not in DECISIONS:
            raise RuleSetError(f"on_timeout must be one of {list(DECISIONS)}, got {on_timeout!r}")
        raw_rules = top["rules"]
        if not isinstance(raw_rules, list):
            raise RuleSetError(f"rules must be a list, got {raw_rules!r}")
        if len(raw_rules) > MAX_RULES:
            raise RuleSetError(f"rules holds {len(raw_rules)} rules, more than {MAX_RULES}")
        ids: dict[str, str] = {}
        rules = tuple(_rule(f"rules[{n}]", raw, ids) for n, raw in enumerate(raw_rules))
    except RuleSetError as exc:
        raise RuleSetError(f"{source}: {exc}") from None

    return RuleSet(
        version=version,
        default="allow",
        on_timeout=on_timeout,
        sha256=hashlib.sha256(data).hexdigest(),
        rules=rules,
    )


def load(path: Path) -> RuleSet:
    try:
        data = path.read_bytes()
    except OSError as exc:
        raise RuleSetError(f"{path}: can't read the rule file: {exc.strerror}") from exc
    return parse(data, str(path))
