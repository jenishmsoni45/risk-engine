"""The PaySim profile (design.md section 8, D32), run on generated data: the report holds the
file's hash and counts that match a direct count of the rows, and a file the loader refuses still
gets a report that says why.
"""

from __future__ import annotations

import hashlib
from collections import Counter
from pathlib import Path

from ml.generate import generate, write_csv
from ml.offline import features
from ml.paysim import load
from ml.profile import RULES, profile
from risk.rules import ruleset


def test_the_report_names_the_file_and_counts_what_it_holds(tmp_path: Path) -> None:
    rows = generate(seed=4)
    csv_path, out = tmp_path / "generated.csv", tmp_path / "reports" / "profile.md"
    write_csv(csv_path, rows)
    assert profile(csv_path, out)
    text = out.read_text()

    assert hashlib.sha256(csv_path.read_bytes()).hexdigest() in text
    by_type = Counter(r[1] for r in rows)
    fraud = Counter(r[1] for r in rows if r[9] == "1")
    for type_, n in by_type.items():
        assert f"| {type_} | {n} | {fraud[type_]} |" in text
    transfers = list(load(csv_path))
    fraud_transfers = sum(t.is_fraud for t in transfers)
    assert f"- Transfers loaded: {len(transfers)}, of which fraud {fraud_transfers} " in text
    assert "Step 1 starts at 2026-01-01T00:00:00Z" in text

    rules = ruleset.load(RULES)
    decisions = Counter(rules.evaluate(row.features)[0] for row in features(transfers))
    for decision in ("allow", "hold", "block"):
        assert f"| {decision} | {decisions[decision]} |" in text


def test_a_file_the_loader_refuses_gets_a_report_saying_why(tmp_path: Path) -> None:
    rows = [
        *generate(seed=4)[:5],
        ("30", "TRANSFER", "1.00", "C5", "0", "0", "C5", "0", "0", "0", "0"),
    ]
    csv_path, out = tmp_path / "bad.csv", tmp_path / "profile.md"
    write_csv(csv_path, rows)
    assert not profile(csv_path, out)
    text = out.read_text()
    assert "send to the same account, which the loader refuses (D52e): 1." in text
    assert "**The loader refuses this file:** line 7: C5 sends to itself" in text
