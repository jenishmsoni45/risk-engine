"""Rule set versions (design.md 1.4, D13, D27). Phase 4, criterion 6: rows store version 1 and the
file's SHA-256; after a restart with a changed file, new decisions carry the new version, and
replays keep their stored ruleSet with the new onTimeout.

The restart is a second API container from the same image, started from the changed file, so the
API every other test uses keeps running (as in tests/integration/test_decision_api.py).
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from tests.helpers import (
    ROOT,
    RULE_FIXTURES,
    api_with_rules,
    database,
    decision_request,
    post_decision,
)

STARTER = ROOT / "risk" / "rules" / "ruleset.yaml"
# Version 3: on_timeout allow, and large_transfer at 500,000 instead of 1,000,000.
CHANGED = RULE_FIXTURES / "starter_changed.yaml"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def stored(request_id: str) -> tuple[Any, ...]:
    with database() as conn:
        row = conn.execute(
            "select decision, fired, rule_set_version, rule_set_hash from decisions "
            "where request_id = %s",
            (request_id,),
        ).fetchone()
    assert row is not None
    return tuple(row)


def answer(body: dict[str, Any], base_url: str | None = None) -> Any:
    response = post_decision(body) if base_url is None else post_decision(body, base_url=base_url)
    assert response.status_code == 200, response.text
    return response.json()


def test_versions_and_hashes_across_a_restart_with_a_changed_file() -> None:
    # Version 1: 600,000 is under large_transfer's threshold, and 1,000,000 is at it.
    allowed, held = decision_request(amount=600_000), decision_request(amount=1_000_000)
    first_allowed, first_held = answer(allowed), answer(held)
    assert first_allowed | {"decisionId": None} == {
        "decisionId": None,
        "decision": "allow",
        "ruleSet": "v1",
        "fired": [],
        "onTimeout": "hold",
    }
    assert first_held | {"decisionId": None} == {
        "decisionId": None,
        "decision": "hold",
        "ruleSet": "v1",
        "fired": ["large_transfer"],
        "onTimeout": "hold",
    }
    starter_rows = {
        allowed["requestId"]: ("allow", [], 1, sha256(STARTER)),
        held["requestId"]: ("hold", ["large_transfer"], 1, sha256(STARTER)),
    }
    assert {request_id: stored(request_id) for request_id in starter_rows} == starter_rows

    with api_with_rules(CHANGED) as restarted:
        replays = [answer(body, restarted) for body in (allowed, held)]
        new = decision_request(amount=600_000)
        new_answer = answer(new, restarted)

    # A replay keeps its stored decision, ruleSet and fired, even where version 3 would decide
    # otherwise, and carries the current rule set's onTimeout (D13).
    assert replays == [first_allowed | {"onTimeout": "allow"}, first_held | {"onTimeout": "allow"}]
    assert {request_id: stored(request_id) for request_id in starter_rows} == starter_rows
    # A new decision is made and stored under version 3, with its file's hash.
    assert new_answer | {"decisionId": None} == {
        "decisionId": None,
        "decision": "hold",
        "ruleSet": "v3",
        "fired": ["large_transfer"],
        "onTimeout": "allow",
    }
    assert stored(new["requestId"]) == ("hold", ["large_transfer"], 3, sha256(CHANGED))
    assert sha256(CHANGED) != sha256(STARTER)

    # The API that kept running still decides under version 1.
    assert answer(decision_request(amount=600_000)) | {"decisionId": None} == {
        **first_allowed,
        "decisionId": None,
    }
