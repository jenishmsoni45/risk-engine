"""Shared test helpers: the repo root, the Compose command and the stack's host-side addresses."""

from __future__ import annotations

import json
import subprocess
from collections.abc import Mapping
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent

# The stack's published addresses on this machine (D31, D40z).
API_URL = "http://127.0.0.1:8000"
STUB_URL = "http://127.0.0.1:8001"
CONSUMER_METRICS_URL = "http://127.0.0.1:8002"
KAFKA_HOST_BOOTSTRAP = "127.0.0.1:19092"
POSTGRES_HOST_PORT = 5433


def env_file() -> Path:
    """The file Compose reads, chosen the same way as the Makefile: .env, else .env.example."""
    local = ROOT / ".env"
    return local if local.exists() else ROOT / ".env.example"


def read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, sep, value = line.partition("=")
        if not sep:
            raise ValueError(f"{path.name}: not KEY=VALUE: {line!r}")
        values[key.strip()] = value.strip()
    return values


def compose(
    *args: str, check: bool = True, timeout: float = 300
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", "compose", "--env-file", str(env_file()), *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=check,
        timeout=timeout,
    )


def compose_ps() -> list[dict[str, Any]]:
    """Every container of the project, running or not, as Compose describes it."""
    out = compose("ps", "--all", "--format", "json").stdout.strip()
    if not out:
        return []
    if out.startswith("["):  # older Compose prints one array
        rows: list[dict[str, Any]] = json.loads(out)
        return rows
    return [json.loads(line) for line in out.splitlines()]


def postgres_conninfo(dbname: str | None = None, env: Mapping[str, str] | None = None) -> str:
    values = env if env is not None else read_env_file(env_file())
    return (
        f"host=127.0.0.1 port={POSTGRES_HOST_PORT} user={values['POSTGRES_USER']} "
        f"password={values['POSTGRES_PASSWORD']} dbname={dbname or values['POSTGRES_DB']} "
        "connect_timeout=5"
    )
