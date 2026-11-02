"""Integration tests run against the Compose stack, which `make test` starts."""

from __future__ import annotations

import pytest


@pytest.fixture(scope="session", autouse=True)
def _needs_stack(stack: None) -> None:
    pass
