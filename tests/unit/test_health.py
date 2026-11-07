"""GET /health answers 503 naming each dependency that doesn't answer; /metrics always answers.

The healthy path runs against the real stack in tests/integration/test_stack.py. Each app runs
in-process with its own startup and shutdown, and httpx calls it directly (D45: no Starlette
TestClient, which is deprecated).
"""

from __future__ import annotations

import asyncio

import httpx
import pytest
from fastapi import FastAPI

from ledger_stub.app import app as stub_app
from risk.api import app as api_app

# Port 1 on the loopback refuses connections at once. Shadow scoring is off, since LightGBM never
# loads on the Mac (D51.4).
UNREACHABLE = {
    "DATABASE_URL": "postgresql://nobody:dummy@127.0.0.1:1/nothing",
    "REDIS_URL": "redis://127.0.0.1:1/0",
    "KAFKA_BOOTSTRAP": "127.0.0.1:1",
    "SHADOW_MODEL_DIR": "",
}


@pytest.fixture(autouse=True)
def unreachable_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    for key, value in UNREACHABLE.items():
        monkeypatch.setenv(key, value)


async def get(app: FastAPI, path: str) -> httpx.Response:
    """GET path from the app, between its startup and its shutdown, as uvicorn would run it."""
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://app") as client:
            return await client.get(path)


@pytest.mark.parametrize(
    ("app", "failed"),
    [(api_app, ["postgres", "redis"]), (stub_app, ["kafka", "postgres"])],
    ids=["api", "stub"],
)
def test_health_names_what_failed(app: FastAPI, failed: list[str]) -> None:
    response = asyncio.run(get(app, "/health"))
    assert response.status_code == 503
    assert response.json() == {"status": "unavailable", "failed": failed}


@pytest.mark.parametrize("app", [api_app, stub_app], ids=["api", "stub"])
def test_metrics_answer_without_dependencies(app: FastAPI) -> None:
    response = asyncio.run(get(app, "/metrics"))
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert "# TYPE" in response.text
