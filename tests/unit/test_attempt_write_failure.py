"""After the decision row commits, the attempt is recorded outside the deadline; if that write
fails the decision is still returned and the failure is counted (design.md 3.3 and section 7,
"Attempt write fails after commit"). A replay records no attempt (D40g).

The decision API runs in-process with a fake engine, so no Redis or Postgres is needed.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import httpx
import pytest
import redis

from risk.api import app
from risk.decisions import Answer, DecisionRequest
from risk.rules.ruleset import RuleSet
from tests.feature_state import sample
from tests.helpers import decision_request

FAILURES = "risk_attempt_write_failures_total"


class FakeEngine:
    def __init__(self, *, replayed: bool, attempt_error: Exception | None) -> None:
        self.replayed = replayed
        self.attempt_error = attempt_error
        self.attempts: list[str] = []

    async def decide(self, request: DecisionRequest) -> Answer:
        return Answer(uuid.uuid4(), "allow", "v1", [], replayed=self.replayed)

    async def record_attempt(self, request: DecisionRequest) -> None:
        self.attempts.append(request.request_id)
        if self.attempt_error is not None:
            raise self.attempt_error


@pytest.fixture
def engine(monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest) -> FakeEngine:
    replayed, error = request.param
    fake = FakeEngine(replayed=replayed, attempt_error=error)
    rules = RuleSet(version=1, default="allow", on_timeout="hold", sha256="0" * 64)
    for name, value in {"engine": fake, "rules": rules, "deadline_s": 0.06}.items():
        monkeypatch.setattr(app.state, name, value, raising=False)
    return fake


async def post(body: dict[str, Any]) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://api") as client:
        return await client.post("/v1/decisions", json=body)


@pytest.mark.parametrize("engine", [(False, redis.ConnectionError("down"))], indirect=True)
def test_a_failed_attempt_write_still_returns_the_decision_and_is_counted(
    engine: FakeEngine,
) -> None:
    before = sample(FAILURES)
    body = decision_request()
    response = asyncio.run(post(body))
    assert response.status_code == 200
    assert response.json()["decision"] == "allow"
    assert engine.attempts == [body["requestId"]]
    assert sample(FAILURES) == before + 1


@pytest.mark.parametrize("engine", [(False, None)], indirect=True)
def test_a_new_decision_records_its_attempt(engine: FakeEngine) -> None:
    body = decision_request()
    assert asyncio.run(post(body)).status_code == 200
    assert engine.attempts == [body["requestId"]]


@pytest.mark.parametrize("engine", [(True, None)], indirect=True)
def test_a_replay_records_no_attempt(engine: FakeEngine) -> None:
    assert asyncio.run(post(decision_request())).status_code == 200
    assert engine.attempts == []
