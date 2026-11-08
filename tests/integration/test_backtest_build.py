"""make backtest's CLI with LightGBM itself, in the image (D51.4, D58k): on a model trained in the
image from generated data, it writes its report and data file, scores with the model the decision
API would load, and gives the same rules line as the run on the Mac. tests/unit/test_backtest.py
covers the comparison itself with stand-in scorers; Phase 8 criterion 3's real report is
`make backtest`'s, committed."""

from __future__ import annotations

import json
import os
from pathlib import Path

import numpy as np

from ml.backtest import Rows, from_data, run
from ml.paysim import load
from ml.profile import sha256
from ml.threshold import Scores, capacity
from tests.helpers import compose
from tests.model_fakes import generated_csv, train_in_image


def any_scores(model: Path, x: Rows) -> tuple[str, Scores]:
    return "any", np.zeros(len(x))


def test_the_backtest_runs_in_the_image_on_a_real_model(tmp_path: Path) -> None:
    csv = generated_csv(tmp_path)
    trained = train_in_image(tmp_path, csv, "trained")
    # Holds the larger half of the transfers, as make paysim-rules would name its CSV.
    median = int(np.median([transfer.amount for transfer in load(csv)]))
    rules = tmp_path / "paysim.yaml"
    rules.write_text(
        f"# Made from {csv.name} (SHA-256 {sha256(csv)}).\n"
        "version: 2\ndefault: allow\non_timeout: hold\nrules:\n"
        "  - id: large_transfer\n    action: hold\n    all:\n"
        f'      - {{ feature: amount, op: ">=", value: {median} }}\n'
    )
    compose(
        "run",
        "--rm",
        "--no-TTY",
        "--user",
        f"{os.getuid()}:{os.getgid()}",
        "--volume",
        f"{tmp_path}:/work",
        "backtest",
        "--csv",
        f"/work/{csv.name}",
        "--rules",
        "/work/paysim.yaml",
        "--model-dir",
        "/work/trained/model",
        "--report",
        "/work/out/backtest.md",
        "--data",
        "/work/out/backtest.json",
        timeout=600,
    )
    result = from_data(json.loads((tmp_path / "out" / "backtest.json").read_text()))
    meta = json.loads((trained / "model" / "model.json").read_text())
    assert result.model_name == meta["name"]
    assert "## On the test days" in (tmp_path / "out" / "backtest.md").read_text()

    rules_line, *model_rows = result.lines
    on_the_mac = run(csv, rules, trained / "model", any_scores)
    assert rules_line == on_the_mac.lines[0]
    assert rules_line.counts.held > 0
    # Each rate gives one line holding exactly its count, or, when a tie at the model's line
    # straddles the count, a line above the tie and one with it (D59).
    rates = {"rules": rules_line.counts.held, "1%": capacity(rules_line.counts.transfers)}
    assert {line.rate for line in model_rows} == set(rates)
    for rate, count in rates.items():
        at = [line for line in model_rows if line.rate == rate]
        if len(at) == 1:
            assert (at[0].side, at[0].counts.held) == ("exact", count)
        else:
            above, with_tie = at
            assert (above.side, with_tie.side) == ("above", "with")
            assert above.counts.held < count < with_tie.counts.held
            assert with_tie.counts.held - above.counts.held == with_tie.tied
