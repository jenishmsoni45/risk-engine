"""Stand-in models for the shadow scorer's tests, and the decision API run in this process.

LightGBM never loads on the Mac (D51.4), so these stand-ins play its part (D58i): each has the two
methods of LightGBM's Booster that the scorer uses. `decision_api` runs the API's own request
handler, with its deadline, against the stack's Postgres and a chosen Redis database, with any
scorer, so a test can compare decisions made with each.
"""

from __future__ import annotations

import time
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import asynccontextmanager
from typing import Any

import httpx
import numpy as np
import numpy.typing as npt
import redis.asyncio as aioredis
from psycopg_pool import AsyncConnectionPool

from risk import settings
from risk.api import app
from risk.decisions import DecisionEngine
from risk.features.compute import FEATURE_NAMES
from risk.features.redis_store import FeatureStore
from risk.rules import ruleset
from risk.shadow import Model, Shadow
from tests.feature_state import redis_url
from tests.helpers import ROOT, postgres_url

STARTER_RULES = ROOT / "risk" / "rules" / "ruleset.yaml"
Rows = npt.NDArray[np.float64]


class StandIn:
    """A model whose answer is `answer(rows)`, with LightGBM's Booster methods."""

    def __init__(
        self, answer: Callable[[Rows], object], names: Sequence[str] = FEATURE_NAMES
    ) -> None:
        self.answer = answer
        self.names = list(names)
        self.params: list[dict[str, Any]] = []

    def predict(self, data: Rows, /, **params: Any) -> object:
        self.params.append(params)
        return self.answer(data)

    def feature_name(self) -> list[str]:
        return list(self.names)


def working(rows: Rows) -> object:
    """A score from 0 to 1 that depends on every feature, as LightGBM returns it for one row."""
    result: Rows = (rows.sum(axis=1) % 997) / 996
    return result


def raising(rows: Rows) -> object:
    raise RuntimeError("the stand-in model failed")


# Answers that aren't one finite number from 0 to 1 (D58d).
NONSENSE: tuple[object, ...] = (
    np.array([np.nan]),
    np.array([np.inf]),
    np.array([-0.25]),
    np.array([1.5]),
    np.array([0.2, 0.3]),
    np.array([[0.5]]),
    np.array([1]),
    "0.5",
    None,
    [0.5],
    True,
)


def nonsense(rows: Rows) -> object:
    """One of NONSENSE, chosen by the features, so a run meets several kinds."""
    return NONSENSE[int(rows.sum()) % len(NONSENSE)]


# Longer than any deadline the tests give the API, so a decision that waited for it would fail.
SLOW_S = 1.0


def slow(rows: Rows) -> object:
    time.sleep(SLOW_S)
    return working(rows)


def stand_in(answer: Callable[[Rows], object], timeout_s: float = 0.01) -> Shadow:
    """Shadow scoring with a stand-in named after its answer, and the default 10 ms limit."""
    name = f"stand-in-{getattr(answer, '__name__', 'model')}"
    return Shadow(Model(name, FEATURE_NAMES, StandIn(answer)), timeout_s)


@asynccontextmanager
async def decision_api(
    shadow: Shadow | None, redis_db: int, deadline_s: float
) -> AsyncIterator[httpx.AsyncClient]:
    """A client of the decision API's handler, run in this process with the starter rules, the
    stack's Postgres, Redis database redis_db and the given scorer; the handler's deadline is
    deadline_s. The app's state is put back afterwards. Closes the scorer at the end."""
    rules = ruleset.load(STARTER_RULES)
    pool = AsyncConnectionPool(
        postgres_url(), min_size=1, max_size=4, kwargs={"autocommit": True}, open=False
    )
    await pool.open(wait=True, timeout=10)
    redis = aioredis.Redis.from_url(redis_url(redis_db), socket_timeout=5)
    engine = DecisionEngine(rules, pool, FeatureStore(redis, settings.lateness_margin_ms()), shadow)
    names = ("engine", "rules", "deadline_s")
    saved = {name: getattr(app.state, name, None) for name in names}
    for name, value in zip(names, (engine, rules, deadline_s), strict=True):
        setattr(app.state, name, value)
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://api") as client:
            yield client
    finally:
        for name, value in saved.items():
            setattr(app.state, name, value)
        if shadow is not None:
            shadow.close()
        await pool.close()
        await redis.aclose()
