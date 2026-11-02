"""The largest 200 body a rule set can produce stays under tally's 16 KiB cap (D13, D40x). Phase 4,
criterion 7: with 100 rules whose 64-character ids all fire, the 200 body is under 16 KiB.
"""

from __future__ import annotations

from pathlib import Path

from risk.rules.ruleset import MAX_RULES, load
from tests.helpers import api_with_rules, decision_request, post_decision

CAP = 16 * 1024


def test_100_fired_rules_with_64_character_ids_fit(tmp_path: Path) -> None:
    ids = [f"rule_{n:03d}_".ljust(64, "x") for n in range(MAX_RULES)]
    # Every rule fires on any request; a version no other rule file uses.
    fires = "[{ feature: amount, op: '>=', value: 1 }]"
    text = "version: 4\ndefault: allow\non_timeout: hold\nrules:\n" + "".join(
        f"  - {{ id: {rule_id}, action: block, all: {fires} }}\n" for rule_id in ids
    )
    # The API container runs as an unprivileged user, so it must be able to read both.
    folder = tmp_path / "rules"
    folder.mkdir()
    folder.chmod(0o755)
    rules_file = folder / "largest.yaml"
    rules_file.write_text(text)
    rules_file.chmod(0o644)
    assert [rule.id for rule in load(rules_file).rules] == ids
    assert {len(rule_id) for rule_id in ids} == {64}

    with api_with_rules(rules_file) as api:
        body = decision_request()
        new = post_decision(body, base_url=api)
        replay = post_decision(body, base_url=api)

    for response in (new, replay):
        assert response.status_code == 200
        assert response.json()["decision"] == "block"
        assert response.json()["fired"] == ids
        assert len(response.content) < CAP
    assert replay.json() == new.json()
