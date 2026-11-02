"""Loading the rule file (design.md 1.4) and evaluating it (design.md section 5).

Phase 2 reads `version`, `default` and `on_timeout` and refuses any rule, because nothing can
evaluate one yet: a rule that loaded and never fired would be worse than a refusal at startup.
Phase 4 adds the rules themselves. Anything wrong stops the service at startup (D27).
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from risk.settings import DECISIONS

KEYS = {"version", "default", "on_timeout", "rules"}


class RuleSetError(Exception):
    """The rule file can't be used; the message names the problem."""


@dataclass(frozen=True)
class RuleSet:
    version: int
    default: str
    on_timeout: str
    sha256: str

    @property
    def name(self) -> str:
        """`ruleSet` in responses (design.md 1.4)."""
        return f"v{self.version}"

    def evaluate(self, features: Mapping[str, int]) -> tuple[str, list[str]]:
        """The decision and the fired rule ids. With no rules, nothing fires: `default`."""
        del features  # read by the rules, which arrive in Phase 4
        return self.default, []


def load(path: Path) -> RuleSet:
    try:
        data = path.read_bytes()
    except OSError as exc:
        raise RuleSetError(f"{path}: can't read the rule file: {exc.strerror}") from exc
    try:
        document: Any = yaml.safe_load(data)
    except yaml.YAMLError as exc:
        raise RuleSetError(f"{path}: not YAML: {exc}") from exc
    if not isinstance(document, dict):
        raise RuleSetError(f"{path}: must be a mapping with the keys {sorted(KEYS)}")

    unknown = sorted(str(key) for key in document if key not in KEYS)
    if unknown:
        raise RuleSetError(f"{path}: unknown keys {unknown}")
    missing = sorted(KEYS - set(document))
    if missing:
        raise RuleSetError(f"{path}: missing keys {missing}")

    version = document["version"]
    # YAML reads `true` as a bool, which Python counts as an int.
    if not isinstance(version, int) or isinstance(version, bool) or version < 1:
        raise RuleSetError(f"{path}: version must be an integer of at least 1, got {version!r}")
    if document["default"] != "allow":
        raise RuleSetError(f"{path}: default must be allow, got {document['default']!r}")
    on_timeout = document["on_timeout"]
    if not isinstance(on_timeout, str) or on_timeout not in DECISIONS:
        raise RuleSetError(
            f"{path}: on_timeout must be one of {list(DECISIONS)}, got {on_timeout!r}"
        )
    rules = document["rules"]
    if not isinstance(rules, list):
        raise RuleSetError(f"{path}: rules must be a list, got {rules!r}")
    if rules:
        raise RuleSetError(f"{path}: rules must be empty: this build can't evaluate rules yet")

    return RuleSet(
        version=version,
        default="allow",
        on_timeout=on_timeout,
        sha256=hashlib.sha256(data).hexdigest(),
    )
