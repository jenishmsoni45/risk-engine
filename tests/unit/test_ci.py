"""What CI runs (Phase 5 criterion 7, Phase 8 criterion 5): I1 to I8 run in CI, because CI runs
`make test`, which runs pytest over tests/, where each invariant's test file sits without any
skip; and CI checks that README.md's tables match the committed results.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent.parent
INVARIANTS = {
    "I1": "test_i1_same_request.py",
    "I2": "test_i2_duplicates_order.py",
    "I3": "test_i3_recompute.py",
    "I4": "test_i4_caller_never_hangs.py",
    "I5": "test_i5_decision_row.py",
    "I6": "test_i6_point_in_time.py",
    "I7": "test_i7_parity.py",
    "I8": "test_i8_shadow.py",
}


def ci_commands() -> list[str]:
    workflow = yaml.safe_load((ROOT / ".github" / "workflows" / "ci.yml").read_text())
    return [step["run"].strip() for step in workflow["jobs"]["ci"]["steps"] if "run" in step]


def test_ci_runs_make_test_and_make_test_runs_pytest_over_tests() -> None:
    assert "make test" in ci_commands()
    makefile = (ROOT / "Makefile").read_text()
    assert re.search(r"^test: .*\n\tuv run --locked pytest\n", makefile, re.MULTILINE)
    pytest_settings = tomllib.loads((ROOT / "pyproject.toml").read_text())["tool"]["pytest"]
    assert pytest_settings["ini_options"]["testpaths"] == ["tests"]


@pytest.mark.parametrize(("invariant", "name"), sorted(INVARIANTS.items()))
def test_each_invariant_test_exists_and_is_never_skipped(invariant: str, name: str) -> None:
    source = (ROOT / "tests" / "invariants" / name).read_text()
    assert re.search(r"^def test_", source, re.MULTILINE), f"{invariant} has no test"
    for marker in ("pytest.skip", "mark.skip", "skipif", "mark.xfail", "importorskip"):
        assert marker not in source, f"{invariant}'s tests use {marker}"


def test_ci_checks_the_readme_against_the_bench_results() -> None:
    assert "make check-readme" in ci_commands()
