"""The report script (Phase 5, criterion 2): bench result files become README.md's tables, with
the first rate whose p99 passes 50 ms or, when errors or the arrival rate gave out first, the
highest rate held (D40o); and `--check` fails when README.md's block differs.

Built on small result folders written here in the shape bench/run.py writes.
"""

from __future__ import annotations

import json
import subprocess
import uuid
from pathlib import Path
from typing import Any

import pytest

from bench import report, run
from risk.features.compute import net_entry
from risk.features.events import parse_event
from risk.timestamps import epoch_ms


def summary(p50: float, p99: float, count: int, failed: float, dropped: int = 0) -> dict[str, Any]:
    metrics: dict[str, Any] = {
        "http_req_duration": {"values": {"p(50)": p50, "p(99)": p99}},
        "http_reqs": {"values": {"count": count}},
        "http_req_failed": {"values": {"rate": failed}},
    }
    if dropped:
        metrics["dropped_iterations"] = {"values": {"count": dropped}}
    return {"metrics": metrics}


def conditions(scenario: str, *, clean: bool = True, step: int = 180) -> dict[str, Any]:
    return {
        "scenario": scenario,
        "git_commit": "0123456789abcdef",
        "git_clean": clean,
        "rule_set_version": 1,
        "host": {"cpu": "Test CPU", "cpus": 10, "memory_bytes": 16 * 1024**3, "os": "TestOS 1"},
        "docker": {"version": "29.0", "vm_cpus": 5, "vm_memory_bytes": 8 * 1024**3},
        "k6": "k6 v2.3.0 (commit/devel)",
        "procedure": {"step_seconds": step},
    }


def write(folder: Path, name: str, document: dict[str, Any]) -> None:
    (folder / name).write_text(json.dumps(document))


def rate_step(
    folder: Path,
    scenario: str,
    rate: int,
    k6: dict[str, Any],
    stopped: str | None = None,
    **stack: Any,
) -> None:
    name = f"{scenario}-{rate:05d}rps"
    write(folder, f"{name}.json", k6)
    write(
        folder,
        f"{name}-stack.json",
        {
            "scenario": scenario,
            "rate": rate,
            "summary": f"{name}.json",
            "stopped": stopped,
            **stack,
        },
    )


STUB_STACK: dict[str, Any] = {
    "stub_transfers": 9000,
    "stub_backup_answers": 9,
    "feature_lag_ms": {"p50": 210, "p99": 480},
    "consumer_offset_lag_at_end": 0,
}


@pytest.fixture
def results(tmp_path: Path) -> Path:
    folder = tmp_path / "2026-11-26"
    folder.mkdir()
    write(folder, "conditions-direct.json", conditions("direct"))
    rate_step(folder, "direct", 50, summary(2.0, 6.0, 9000, 0.0))
    rate_step(folder, "direct", 100, summary(2.5, 61.25, 18000, 0.0), "p99 passed 50 ms")
    write(folder, "conditions-stub.json", conditions("stub"))
    rate_step(folder, "stub", 50, summary(5.0, 9.0, 9000, 0.001), **STUB_STACK)
    rate_step(
        folder, "stub", 100, summary(6.0, 20.0, 18000, 0.02), "errors passed 1%", **STUB_STACK
    )
    write(folder, "conditions-hot.json", conditions("hot"))
    for size, p99, stopped in ((1000, 8.0, None), (10000, 75.5, "p99 passed 50 ms")):
        name = f"hot-{size:07d}"
        write(folder, f"{name}.json", summary(3.0, p99, 9000, 0.0))
        write(
            folder,
            f"{name}-stack.json",
            {
                "scenario": "hot",
                "rate": 50,
                "preloaded": size,
                "summary": f"{name}.json",
                "members_read": {"p50": size + 4500},
                "stopped": stopped,
            },
        )
    return folder


def test_the_tables_state_each_rate_and_the_crossing(results: Path) -> None:
    block, problems = report.render(results)
    assert problems == []
    assert block.startswith(report.START)
    assert block.endswith(report.END)
    assert "| 50 | 9000 | 2.0 | 6.0 | 0.00% | 0 |" in block
    assert "| 100 | 18000 | 2.5 | 61.2 | 0.00% | 0 |" in block
    assert "p99 first passes 50 ms at **100 requests a second**." in block


def test_errors_before_the_crossing_name_the_highest_rate_held(results: Path) -> None:
    block, _ = report.render(results)
    assert (
        "The run stopped at 100 transfers a second because errors passed 1%, before p99 passed "
        "50 ms; the highest rate held was 50 transfers a second." in block
    )
    # The timeout rate is the stub's backup answers over its transfers: 9 of 9000.
    assert "| 50 | 9000 | 5.0 | 9.0 | 0.10% | 0.10% | 210 | 480 | 0 |" in block


def test_the_hot_account_table_shows_p99_against_the_window(results: Path) -> None:
    block, _ = report.render(results)
    assert "#### Hot account: one receiver, 50 requests a second" in block
    assert "| 1000 | 5500 | 3.0 | 8.0 | 0.00% |" in block
    assert (
        "p99 first passes 50 ms with **10000 preloaded events** in the receiver's 24 h window."
        in block
    )


def test_a_preloaded_event_adds_only_to_the_hot_accounts_incoming_window() -> None:
    """D50: the preload fills the hot receiver's incoming set, and the consumer's own parsing
    and netting take the event as tally's: an inflow of 100 to the hot account, an outflow from
    one of the bench's accounts, at the createdAt given."""
    hot = uuid.uuid4()
    created_ms = 1_798_000_000_123
    for number in (0, 1, run.ACCOUNTS - 1, run.ACCOUNTS, 999_999):
        entry_id, document = run.hot_event(hot, number, created_ms)
        event = parse_event(entry_id.encode(), json.dumps(document).encode())
        assert epoch_ms(event.created_at) == created_ms
        movements = {m.account_id: (m.direction, m.amount) for m in net_entry(event)}
        sender = uuid.UUID(run.account(1 + number % run.ACCOUNTS))
        assert movements == {hot: ("in", 100), sender: ("out", 100)}


def test_the_conditions_are_listed(results: Path) -> None:
    block, _ = report.render(results)
    assert "Test CPU, 10 cores, 16 GiB" in block
    assert "commit `0123456`, rule set v1; k6 v2.3.0; steps of 180 s." in block


@pytest.mark.parametrize(
    ("change", "problem"),
    [
        ({"clean": False}, "uncommitted changes"),
        ({"step": 10}, "steps were 10 s"),
    ],
)
def test_results_from_a_dirty_tree_or_short_steps_never_go_in_the_readme(
    results: Path, tmp_path: Path, change: dict[str, Any], problem: str
) -> None:
    write(results, "conditions-direct.json", conditions("direct", **change))
    readme = tmp_path / "README.md"
    readme.write_text(f"intro\n{report.START}\n{report.END}\n")
    assert report.main(["--results", str(results), "--write", str(readme)]) == 1
    assert readme.read_text() == f"intro\n{report.START}\n{report.END}\n"
    _, problems = report.render(results)
    assert any(problem in p for p in problems)


def test_check_passes_after_write_and_fails_after_an_edit(results: Path, tmp_path: Path) -> None:
    readme = tmp_path / "README.md"
    readme.write_text(f"# Title\n\n{report.START}\nold\n{report.END}\n\nafter\n")
    assert report.main(["--results", str(results), "--write", str(readme)]) == 0
    text = readme.read_text()
    assert text.startswith("# Title\n\n")
    assert text.endswith("\n\nafter\n")
    assert report.main(["--results", str(results), "--check", str(readme)]) == 0

    readme.write_text(text.replace("| 2.0 | 6.0 |", "| 2.0 | 5.0 |"))
    assert report.main(["--results", str(results), "--check", str(readme)]) == 1


def test_a_readme_without_one_block_is_refused(results: Path, tmp_path: Path) -> None:
    readme = tmp_path / "README.md"
    readme.write_text("no markers\n")
    assert report.main(["--results", str(results), "--check", str(readme)]) == 1


def test_the_newest_folder_is_read(tmp_path: Path) -> None:
    for name in ("2026-11-26", "2026-11-28", "2026-11-27"):
        (tmp_path / name).mkdir()
        write(tmp_path / name, "conditions-direct.json", conditions("direct"))
    (tmp_path / "2026-11-29").mkdir()  # no conditions file: not a results folder
    assert report.latest(tmp_path).name == "2026-11-28"


def test_quantiles_by_nearest_rank() -> None:
    values = list(range(1, 101))
    assert run.nearest_rank(values, 0.5) == 50
    assert run.nearest_rank(values, 0.99) == 99
    assert run.nearest_rank([7], 0.99) == 7
    assert run.nearest_rank([], 0.5) is None


def test_a_runs_own_results_leave_the_tree_clean_but_any_other_change_does_not(
    tmp_path: Path,
) -> None:
    """The stub and hot scenarios run after direct has written its files to bench/results/, so
    those files mustn't mark them as run from a changed tree; anything else must."""

    def git(*args: str) -> None:
        subprocess.run(
            ["git", "-c", "user.name=t", "-c", "user.email=t@example.invalid", *args],
            cwd=tmp_path,
            check=True,
            capture_output=True,
        )

    git("init", "--quiet")
    (tmp_path / "code.py").write_text("x = 1\n")
    git("add", "code.py")
    git("commit", "--quiet", "--message", "start")
    assert run.tree_clean(tmp_path)

    folder = tmp_path / "bench" / "results" / "2026-11-28"
    folder.mkdir(parents=True)
    (folder / "conditions-direct.json").write_text("{}\n")
    assert run.tree_clean(tmp_path)

    (tmp_path / "notes.txt").write_text("new\n")
    assert not run.tree_clean(tmp_path)
    (tmp_path / "notes.txt").unlink()

    (tmp_path / "code.py").write_text("x = 2\n")
    assert not run.tree_clean(tmp_path)


def test_a_failing_git_never_counts_as_a_clean_tree(tmp_path: Path) -> None:
    assert not run.tree_clean(tmp_path / "missing")
