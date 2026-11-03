"""No secrets or data in the repo (CLAUDE.md): .env and data/ ignored, .env.example dummy-only."""

from __future__ import annotations

import re
import subprocess

import pytest

from ml.paysim import COLUMNS
from tests.helpers import ROOT, read_env_file


def git(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True, check=False)


@pytest.mark.parametrize("path", [".env", "data/PS_20174392719_1491204439457_log.csv", "data/x"])
def test_path_is_git_ignored(path: str) -> None:
    # --no-index: judge by the ignore rules alone, even for a path that is already tracked.
    assert git("check-ignore", "--quiet", "--no-index", path).returncode == 0, path


def test_nothing_under_data_or_env_is_tracked() -> None:
    tracked = git("ls-files", "--", ".env", "data").stdout.split()
    assert tracked == []


def test_no_tracked_file_holds_paysim_rows() -> None:
    """Phase 6, criterion 9, wherever a CSV might land: no tracked file starts a PaySim table,
    in data/, ml/reports/ or anywhere else (CLAUDE.md). The header is built here, so this file
    doesn't hold it either."""
    header = ",".join(COLUMNS)
    found = git("grep", "--fixed-strings", "--files-with-matches", header)
    assert found.returncode == 1, found.stdout  # 1: no tracked file matches


def test_env_example_is_tracked_and_not_ignored() -> None:
    assert git("ls-files", "--error-unmatch", ".env.example").returncode == 0
    assert git("check-ignore", "--quiet", "--no-index", ".env.example").returncode == 1


SECRET_KEY = re.compile(r"PASSWORD|SECRET|TOKEN|API_KEY|PRIVATE", re.IGNORECASE)


def test_env_example_holds_only_dummy_values() -> None:
    values = read_env_file(ROOT / ".env.example")
    assert values, ".env.example is empty"
    secrets = {key: value for key, value in values.items() if SECRET_KEY.search(key)}
    assert secrets, "expected at least the Postgres password"
    for key, value in secrets.items():
        assert "dummy" in value, f"{key} must be an obvious dummy value"
    for key, value in values.items():
        assert "://" not in value or "@" not in value, f"{key}: no credentials inside URLs"
