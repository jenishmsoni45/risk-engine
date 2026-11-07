"""D41's install safeguards stay on: the cutoff, wheels only, no Python downloads, approved
packages only, every locked package from PyPI with a wheel, and locked installs everywhere."""

from __future__ import annotations

import re
import tomllib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from tests.helpers import ROOT

PYPI = "https://pypi.org/simple"
WHEEL_HOST = "https://files.pythonhosted.org/"

# D40s's list, by group. httpx is a runtime library that only the tests use until Phase 2.
RUNTIME = {
    "fastapi",
    "uvicorn",
    "pydantic",
    "redis",
    "psycopg",
    "confluent-kafka",
    "httpx",
    "pyyaml",
    "prometheus-client",
}
DEV = {"pytest", "hypothesis", "ruff", "mypy", "pip-audit"}
# numpy builds LightGBM's feature array (D55.2).
TWO_B = {"lightgbm", "pandas", "numpy"}

PINNED = re.compile(r"(?P<name>[A-Za-z0-9._-]+)(?:\[(?P<extras>[^\]]+)\])?==(?P<version>[^\s;]+)")
FIXED_UTC = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z")


def normalize(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def load_toml(name: str) -> dict[str, Any]:
    with (ROOT / name).open("rb") as f:
        return tomllib.load(f)


@pytest.fixture(scope="module")
def pyproject() -> dict[str, Any]:
    return load_toml("pyproject.toml")


@pytest.fixture(scope="module")
def lock() -> dict[str, Any]:
    return load_toml("uv.lock")


def uv_settings(pyproject: dict[str, Any]) -> dict[str, Any]:
    settings: dict[str, Any] = pyproject["tool"]["uv"]
    return settings


def cutoff(pyproject: dict[str, Any]) -> datetime:
    return datetime.fromisoformat(uv_settings(pyproject)["exclude-newer"])


# --- pyproject.toml settings (D41.1 to D41.3) -----------------------------------------------


def test_release_cutoff_is_a_fixed_utc_time_at_least_7_days_old(pyproject: dict[str, Any]) -> None:
    value = uv_settings(pyproject)["exclude-newer"]
    assert isinstance(value, str)
    assert FIXED_UTC.fullmatch(value), f"exclude-newer must be a fixed UTC time, got {value!r}"
    assert cutoff(pyproject) <= datetime.now(UTC) - timedelta(days=7)


def test_every_cutoff_override_has_a_decision(pyproject: dict[str, Any]) -> None:
    overrides: dict[str, str] = uv_settings(pyproject).get("exclude-newer-package", {})
    decisions = (ROOT / "docs" / "decisions.md").read_text()
    for package in overrides:
        assert package in decisions, f"the cutoff override for {package} needs a decisions.md entry"


def test_wheels_only_no_python_downloads_not_a_package(pyproject: dict[str, Any]) -> None:
    settings = uv_settings(pyproject)
    assert settings["no-build"] is True
    assert settings["python-downloads"] == "never"
    assert settings["package"] is False
    assert "build-system" not in pyproject


def test_packages_come_only_from_pypi(pyproject: dict[str, Any]) -> None:
    settings = uv_settings(pyproject)
    for key in ("index", "index-url", "extra-index-url", "find-links", "sources"):
        assert key not in settings, f"[tool.uv] {key} would add a package source besides PyPI"


# --- Direct dependencies (D41.5) -------------------------------------------------------------


def parse_pins(requirements: list[str]) -> dict[str, tuple[set[str], str]]:
    pins: dict[str, tuple[set[str], str]] = {}
    for requirement in requirements:
        match = PINNED.fullmatch(requirement)
        assert match, f"{requirement!r} must be pinned exactly with =="
        extras = {e.strip() for e in (match["extras"] or "").split(",") if e.strip()}
        pins[normalize(match["name"])] = (extras, match["version"])
    return pins


def test_runtime_dependencies_are_on_the_approved_list(pyproject: dict[str, Any]) -> None:
    pins = parse_pins(pyproject["project"]["dependencies"])
    assert set(pins) <= RUNTIME | TWO_B, set(pins) - (RUNTIME | TWO_B)


def test_dev_dependencies_are_on_the_approved_list(pyproject: dict[str, Any]) -> None:
    groups: dict[str, list[str]] = pyproject.get("dependency-groups", {})
    assert set(groups) <= {"dev"}, "only the dev group is approved"
    pins = parse_pins(groups.get("dev", []))
    assert set(pins) <= DEV | RUNTIME, set(pins) - (DEV | RUNTIME)


def test_psycopg_carries_its_own_libpq(pyproject: dict[str, Any]) -> None:
    pins = parse_pins(pyproject["project"]["dependencies"])
    if "psycopg" in pins:
        assert "binary" in pins["psycopg"][0], "install psycopg as psycopg[binary] (D41.5)"


# --- uv.lock (D41.1, D41.2) ------------------------------------------------------------------


def locked_packages(lock: dict[str, Any]) -> list[dict[str, Any]]:
    packages: list[dict[str, Any]] = lock["package"]
    return packages


def test_lock_was_resolved_with_the_cutoff(pyproject: dict[str, Any], lock: dict[str, Any]) -> None:
    assert lock["options"]["exclude-newer"] == uv_settings(pyproject)["exclude-newer"]


def test_every_locked_package_comes_from_pypi_with_a_wheel(lock: dict[str, Any]) -> None:
    for package in locked_packages(lock):
        name = package["name"]
        if package["source"] == {"virtual": "."}:
            assert name == "risk-engine"
            continue
        assert package["source"] == {"registry": PYPI}, name
        wheels = package.get("wheels", [])
        assert wheels, f"{name} has no wheel"
        for wheel in wheels:
            assert wheel["url"].startswith(WHEEL_HOST), name
            assert wheel["hash"].startswith("sha256:"), name


def test_every_locked_file_predates_the_cutoff(
    pyproject: dict[str, Any], lock: dict[str, Any]
) -> None:
    overrides = set(uv_settings(pyproject).get("exclude-newer-package", {}))
    limit = cutoff(pyproject)
    for package in locked_packages(lock):
        if package["name"] in overrides or "wheels" not in package:
            continue
        for wheel in package["wheels"]:
            uploaded = datetime.fromisoformat(wheel["upload-time"])
            assert uploaded <= limit, f"{package['name']}: {wheel['url']} is newer than the cutoff"


# --- Installs in the Makefile, the Dockerfile and CI (D41.4, D42) --------------------------

INSTALL_FILES = ["Makefile", "Dockerfile", ".github/workflows/ci.yml"]
FORBIDDEN = {
    "uv pip": re.compile(r"\buv\s+pip\b"),
    "uv add": re.compile(r"\buv\s+add\b"),
    "uv tool": re.compile(r"\buv\s+tool\b"),
    "uvx": re.compile(r"\buvx\b"),
    "uv lock without --check": re.compile(r"\buv\s+lock\b(?![^\n]*--check)"),
}
PIP_INSTALL = re.compile(r"\bpip3?\s+install\b|-m\s+pip\s+install\b")


def logical_lines(path: Path) -> list[str]:
    """Lines with shell and Make continuations joined, so each command is one line."""
    return path.read_text().replace("\\\n", " ").splitlines()


@pytest.mark.parametrize("name", INSTALL_FILES)
def test_uv_installs_are_locked(name: str) -> None:
    for line in logical_lines(ROOT / name):
        for command in re.findall(r"\buv\s+(?:sync|run)\b[^\n;&|]*", line):
            assert "--locked" in command, f"{name}: {command.strip()!r} must use --locked"
        for what, pattern in FORBIDDEN.items():
            assert not pattern.search(line), f"{name}: {what} is not allowed: {line.strip()!r}"


def test_dockerfile_and_makefile_install_with_uv_sync_locked() -> None:
    for name in ("Dockerfile", "Makefile"):
        assert re.search(r"\buv\s+sync\s+--locked\b", (ROOT / name).read_text()), name


@pytest.mark.parametrize("name", ["Makefile", ".github/workflows/ci.yml"])
def test_no_pip_installs_outside_the_image(name: str) -> None:
    for line in logical_lines(ROOT / name):
        assert not PIP_INSTALL.search(line), f"{name}: {line.strip()!r}"


def test_the_image_bootstraps_only_hash_pinned_uv() -> None:
    pip_lines = [line for line in logical_lines(ROOT / "Dockerfile") if PIP_INSTALL.search(line)]
    assert len(pip_lines) == 1, pip_lines
    line = pip_lines[0]
    for flag in ("--require-hashes", "--only-binary :all:", "--no-deps", "uv-requirements.txt"):
        assert flag in line, f"the uv bootstrap needs {flag}"
    requirements = [
        line.strip()
        for line in logical_lines(ROOT / "docker" / "uv-requirements.txt")
        if line.strip() and not line.lstrip().startswith("#")
    ]
    assert len(requirements) == 1, requirements
    assert re.fullmatch(r"uv==[\d.]+(\s+--hash=sha256:[0-9a-f]{64})+", requirements[0])


def test_ci_actions_are_pinned_to_commits_and_uv_matches_the_image() -> None:
    ci = (ROOT / ".github" / "workflows" / "ci.yml").read_text()
    uses = re.findall(r"uses:\s*(\S+)", ci)
    assert uses
    for action in uses:
        assert re.fullmatch(r"[\w.-]+/[\w.-]+@[0-9a-f]{40}", action), f"pin {action} to a commit"
    ci_uv = re.search(r"version:\s*\"?([\d.]+)\"?", ci)
    image_uv = re.search(r"uv==([\d.]+)", (ROOT / "docker" / "uv-requirements.txt").read_text())
    assert ci_uv, "CI must pin uv's version"
    assert image_uv, "the image must pin uv's version"
    assert ci_uv[1] == image_uv[1]
    assert re.search(r"checksum:\s*\"?[0-9a-f]{64}\"?", ci), "CI's uv download needs its checksum"
