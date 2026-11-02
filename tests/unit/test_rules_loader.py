"""The rule file loader (design.md 1.4, D27). Phase 2's file holds version, default and
on_timeout, and no rules; a file that breaks any of that stops the service at startup.
Phase 4 adds the rules and their checks here.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from risk.rules.ruleset import RuleSetError, load
from tests.helpers import ROOT

REPO_RULES = ROOT / "risk" / "rules" / "ruleset.yaml"
GOOD = "version: 1\ndefault: allow\non_timeout: hold\nrules: []\n"


def write(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "ruleset.yaml"
    path.write_text(text)
    return path


def test_the_repo_rule_set() -> None:
    rules = load(REPO_RULES)
    assert (rules.version, rules.default, rules.on_timeout) == (1, "allow", "hold")
    assert rules.name == "v1"
    assert rules.sha256 == hashlib.sha256(REPO_RULES.read_bytes()).hexdigest()
    assert rules.evaluate({"amount": 1}) == ("allow", [])


@pytest.mark.parametrize("on_timeout", ["allow", "hold", "block"])
def test_each_on_timeout(tmp_path: Path, on_timeout: str) -> None:
    text = GOOD.replace("hold", on_timeout)
    assert load(write(tmp_path, text)).on_timeout == on_timeout


def test_the_hash_is_of_the_file_bytes(tmp_path: Path) -> None:
    one = load(write(tmp_path, GOOD)).sha256
    # The same rule set spelled differently is a different file, so a different hash.
    two = load(write(tmp_path, "# a comment\n" + GOOD)).sha256
    assert one != two


BROKEN = {
    "not YAML": ("version: [1\n", "not YAML"),
    "not a mapping": ("- 1\n- 2\n", "mapping"),
    "an unknown key": (GOOD + "extra: 1\n", "unknown keys"),
    "version missing": (GOOD.replace("version: 1\n", ""), "missing keys"),
    "rules missing": (GOOD.replace("rules: []\n", ""), "missing keys"),
    "version zero": (GOOD.replace("version: 1", "version: 0"), "version"),
    "version a float": (GOOD.replace("version: 1", "version: 1.5"), "version"),
    "version a string": (GOOD.replace("version: 1", "version: '1'"), "version"),
    "version true": (GOOD.replace("version: 1", "version: true"), "version"),
    "default hold": (GOOD.replace("default: allow", "default: hold"), "default"),
    "on_timeout unknown": (GOOD.replace("on_timeout: hold", "on_timeout: deny"), "on_timeout"),
    # YAML 1.1 reads a bare no as false.
    "on_timeout no": (GOOD.replace("on_timeout: hold", "on_timeout: no"), "on_timeout"),
    "on_timeout uppercase": (GOOD.replace("on_timeout: hold", "on_timeout: HOLD"), "on_timeout"),
    "rules not a list": (GOOD.replace("rules: []", "rules: {}"), "rules"),
    "a rule": (GOOD.replace("rules: []", "rules:\n  - id: r\n    action: hold\n"), "rules"),
}


@pytest.mark.parametrize(("text", "named"), BROKEN.values(), ids=BROKEN.keys())
def test_a_broken_file_names_its_problem(tmp_path: Path, text: str, named: str) -> None:
    with pytest.raises(RuleSetError, match=named):
        load(write(tmp_path, text))


def test_a_missing_file(tmp_path: Path) -> None:
    with pytest.raises(RuleSetError, match="can't read"):
        load(tmp_path / "nope.yaml")
