"""An invalid rule file stops the decision API at startup (D27; design.md section 7, "Invalid rule
file"). Phase 4, criterion 1's container-level check: the API's own container, started from a file
that names an unknown feature, exits before it serves anything, with a message naming the problem.
tests/unit/test_rules_loader.py covers every kind of invalid file.
"""

from __future__ import annotations

import subprocess

from tests.helpers import RULE_FIXTURES, api_run_args, compose, unique, wait_until_removed


def test_an_invalid_rule_file_stops_the_api() -> None:
    name = unique("risk-engine-api-invalid-rules")
    try:
        # In the foreground: the call returns when the container exits, with its exit code.
        result = compose(
            *api_run_args(name, RULE_FIXTURES / "unknown_feature.yaml"),
            "--no-TTY",
            "api",
            check=False,
            timeout=120,
        )
    finally:
        subprocess.run(["docker", "stop", "--time", "5", name], capture_output=True, check=False)
        wait_until_removed(name)
    output = result.stdout + result.stderr
    assert result.returncode != 0, output
    assert (
        "invalid rule file, not starting: /rules/unknown_feature.yaml: "
        "rules[0] (velocity_2m).all[0]: unknown feature 'out_count_2m'"
    ) in output
    # uvicorn listens only after startup succeeds, so the API never served a request.
    assert "Application startup failed" in output
    assert "Uvicorn running" not in output
