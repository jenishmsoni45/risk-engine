"""The stub's limits that copy tally's (D48.5): at most 100 decision calls in flight, a call over
that being no decision at once; and no access log line per request on the stub or the API.

The cap is checked with a lower limit against the fake decision API on the loopback.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from pathlib import Path
from typing import Any

import pytest
import uvicorn
import yaml

from ledger_stub import risk_client
from ledger_stub.risk_client import Decided, NoDecision, RiskClient
from risk import api
from tests.fake_risk import FakeRisk, decided, late

ROOT = Path(__file__).resolve().parent.parent.parent


def test_the_cap_is_tallys() -> None:
    assert risk_client.MAX_IN_FLIGHT == 100
    assert RiskClient("http://127.0.0.1:1", 0.1, "hold").max_in_flight == 100


def test_a_call_over_the_cap_is_no_decision_at_once_and_a_freed_slot_is_used_again() -> None:
    async def scenario(fake: FakeRisk) -> None:
        client = RiskClient(fake.url, 1.0, "hold", max_in_flight=2)

        async def ask() -> object:
            return await client.decide("key", uuid.uuid4(), uuid.uuid4(), 100, "CAD")

        try:
            in_flight = [asyncio.create_task(ask()) for _ in range(2)]
            deadline = time.monotonic() + 2
            while fake.calls < 2:
                assert time.monotonic() < deadline, "the two calls never reached the fake"
                await asyncio.sleep(0.01)
            assert client.in_flight == 2

            started = time.monotonic()
            over = await ask()
            elapsed = time.monotonic() - started
            assert over == NoDecision("unavailable")
            assert elapsed < 0.05, f"the call over the cap took {elapsed * 1000:.0f} ms"

            assert all(isinstance(r, Decided) for r in await asyncio.gather(*in_flight))
            assert client.in_flight == 0
            assert isinstance(await ask(), Decided)
            # The call over the cap never reached the decision API.
            assert fake.calls == 3
        finally:
            await client.aclose()

    with FakeRisk() as fake:
        fake.respond_with(late(0.3, decided()))
        asyncio.run(scenario(fake))


def test_the_stub_writes_no_access_log() -> None:
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
    command = compose["services"]["stub"]["command"]
    assert command[0] == "uvicorn"
    assert "--no-access-log" in command


def test_the_api_writes_no_access_log(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(uvicorn, "run", lambda _app, **kwargs: calls.append(kwargs))
    api.main()
    assert calls[0]["access_log"] is False
