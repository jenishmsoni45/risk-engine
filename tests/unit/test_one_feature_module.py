"""One feature module (CLAUDE.md, design.md 3.5). Phase 6, criterion 8: the offline pipeline and
the decision API compute features with the same function from risk/features/, checked by
identity on the imported functions, and nothing under ml/ defines feature logic of its own.
"""

from __future__ import annotations

import ast
import inspect
from pathlib import Path

import ml.offline
import risk.decisions
import risk.features.consumer
from risk.features import compute
from tests.helpers import ROOT

# The shared module's feature logic (design.md 3.5), the functions it defines: what no other
# module may define again.
FEATURE_LOGIC = {
    name
    for name, value in vars(compute).items()
    if inspect.isfunction(value) and value.__module__ == compute.__name__
}


def test_the_offline_pipeline_and_the_api_compute_features_with_one_function() -> None:
    assert ml.offline.features_at is compute.features_at
    assert risk.decisions.features_at is compute.features_at


def test_the_offline_pipeline_and_the_consumer_net_entries_with_one_function() -> None:
    assert ml.offline.net_entry is compute.net_entry
    assert risk.features.consumer.net_entry is compute.net_entry


def test_the_hour_of_day_comes_from_the_shared_module() -> None:
    assert ml.offline.hour_of_day is compute.hour_of_day


def test_nothing_under_ml_defines_its_own_feature_logic() -> None:
    assert {"features_at", "net_entry", "in_window", "hour_of_day"} <= FEATURE_LOGIC
    for path in sorted(Path(ROOT / "ml").rglob("*.py")):
        tree = ast.parse(path.read_text(), filename=str(path))
        defined = {
            node.name
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef)
        }
        assert not defined & FEATURE_LOGIC, (path.name, defined & FEATURE_LOGIC)
