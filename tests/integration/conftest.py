"""Integration tests run against the Compose stack, which `make test` starts."""

from __future__ import annotations

import pytest

from tests.helpers import compose_ps


@pytest.fixture(scope="session", autouse=True)
def stack() -> None:
    running = [row["Service"] for row in compose_ps() if row.get("State") == "running"]
    if not running:
        pytest.fail("the Compose stack isn't running: start it with `make up` (or run `make test`)")
