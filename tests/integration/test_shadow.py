"""Each decision row stores the shadow score and the model's name; a scoring failure stores null
and is counted (Phase 8, criterion 2; design.md section 8; D58c, D58j).

- The stack's API scores the committed model. Each row's score must equal LightGBM's own score of
  the row's stored features, recomputed in the image (LightGBM never loads on the Mac, D51.4)
  straight from model.txt, in the order the model file names its features, so the check doesn't
  share the scorer's code.
- A second API, started with a model that fails its checks, still decides; its rows hold no score
  and no model name, and it counts each decision as `unavailable`.
- A model that raises: the decision API's handler, run in this process with a stand-in model,
  stores the model's name and no score, and counts an `error`.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import shutil
import subprocess
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest
import redis

from risk.features.compute import FAMILIES
from tests.feature_state import SCRATCH_DB, sample
from tests.helpers import (
    API_URL,
    ROOT,
    database,
    decision_request,
    env_file,
    metric,
    post_decision,
    publish,
    second_api,
    wait_until_consumed,
)
from tests.rule_scenario import Scenario
from tests.shadow_fakes import decision_api, raising, stand_in

MODEL = ROOT / "ml" / "model"
FAILURES = "risk_shadow_failures_total"

# Runs in the image: LightGBM's score of each line's features, from model.txt alone.
RECOMPUTE = """
import json, sys
import lightgbm, numpy as np
booster = lightgbm.Booster(model_file="ml/model/model.txt")
names = booster.feature_name()
for line in sys.stdin:
    features = json.loads(line)
    row = np.array([[features[name] for name in names]], dtype=np.float64)
    print(repr(float(booster.predict(row)[0])))
"""


def scores_in_image(snapshots: Sequence[dict[str, int]]) -> list[float]:
    done = subprocess.run(
        [
            "docker",
            "compose",
            "--env-file",
            str(env_file()),
            "run",
            "--rm",
            "--no-deps",
            "--no-TTY",
            "api",
            "python",
            "-c",
            RECOMPUTE,
        ],
        input="".join(json.dumps(s) + "\n" for s in snapshots),
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    assert done.returncode == 0, done.stderr
    return [float(line) for line in done.stdout.split()]


def rows(request_ids: Sequence[str]) -> list[tuple[Any, ...]]:
    """(features, shadow_score, shadow_model) of each request's row, in the order given."""
    with database() as conn:
        found = conn.execute(
            "select request_id, features, shadow_score, shadow_model from decisions "
            "where request_id = any(%s)",
            (list(request_ids),),
        ).fetchall()
    by_id = {row[0]: tuple(row[1:]) for row in found}
    return [by_id[request_id] for request_id in request_ids]


def test_each_row_stores_the_models_name_and_its_score() -> None:
    meta = json.loads((MODEL / "model.json").read_text())
    scenario = Scenario()
    wait_until_consumed(publish([(e.key(), e.value()) for e in scenario.events()]))
    bodies = scenario.requests()
    assert [post_decision(body).status_code for body in bodies] == [200] * len(bodies)

    stored = rows([body["requestId"] for body in bodies])
    assert {model for _, _, model in stored} == {meta["name"]}
    snapshots = [features for features, _, _ in stored]
    # Every feature family has members for some request, so the inputs' order is checked.
    for family in FAMILIES:
        assert any(s[f"{family}_count_24h"] > 0 for s in snapshots), family
    assert [score for _, score, _ in stored] == scores_in_image(snapshots)


def broken_model(folder: Path, kind: str) -> Path:
    """A copy of the committed model that fails one of D58b's checks, readable by the API's
    unprivileged user."""
    folder.mkdir(mode=0o755)
    shutil.copy(MODEL / "model.json", folder / "model.json")
    meta = json.loads((MODEL / "model.json").read_text())
    if kind == "changed":  # model.txt no longer matches the SHA-256 in model.json
        (folder / "model.txt").write_text((MODEL / "model.txt").read_text() + "\n")
    elif kind == "not a model":  # matches its SHA-256, but LightGBM can't read it
        (folder / "model.txt").write_text("not a model\n")
        meta["sha256"] = hashlib.sha256(b"not a model\n").hexdigest()
        (folder / "model.json").write_text(json.dumps(meta))
    elif kind == "missing":
        pass
    for path in folder.iterdir():
        path.chmod(0o644)
    folder.chmod(0o755)
    return folder


@pytest.mark.parametrize("kind", ["changed", "not a model", "missing"])
def test_a_model_that_fails_to_load_stores_no_score_and_is_counted(
    tmp_path: Path, kind: str
) -> None:
    folder = broken_model(tmp_path / "model", kind)
    with second_api(env={"SHADOW_MODEL_DIR": "/model"}, volumes=[f"{folder}:/model:ro"]) as api:
        bodies = [decision_request(), decision_request(amount=1_000_000)]
        answers = [post_decision(body, base_url=api.url) for body in bodies]
        assert [a.status_code for a in answers] == [200, 200]
        assert [a.json()["decision"] for a in answers] == ["allow", "hold"]
        assert metric(api.url, FAILURES, reason="unavailable") == 2
        assert "shadow model not loaded" in api.logs()
    assert [row[1:] for row in rows([b["requestId"] for b in bodies])] == [(None, None)] * 2


def test_the_stacks_api_counts_no_failure() -> None:
    before = {
        reason: metric(API_URL, FAILURES, reason=reason)
        for reason in ("unavailable", "error", "invalid", "timeout")
    }
    assert post_decision(decision_request()).status_code == 200
    after = {reason: metric(API_URL, FAILURES, reason=reason) for reason in before}
    assert after == before


def test_a_model_that_raises_stores_no_score_and_is_counted(scratch: redis.Redis) -> None:
    async def decide(body: dict[str, Any]) -> int:
        async with decision_api(stand_in(raising), SCRATCH_DB, 0.5) as client:
            response = await client.post("/v1/decisions", json=body)
            return response.status_code

    before = sample(FAILURES, reason="error")
    body = decision_request()
    assert asyncio.run(decide(body)) == 200
    assert sample(FAILURES, reason="error") == before + 1
    assert rows([body["requestId"]])[0][1:] == (None, "stand-in-raising")
