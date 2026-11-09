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
from ml.backtest import Line, Result, data
from ml.metrics import AtThreshold
from risk.features.compute import net_entry
from risk.features.events import parse_event
from risk.timestamps import epoch_ms


def summary(
    p50: float,
    p99: float,
    count: int,
    failed: float,
    dropped: int = 0,
    run_ms: float | None = 180_000.5,
) -> dict[str, Any]:
    """k6's summary of a step; run_ms is how long k6 says it ran, None for a summary without it."""
    metrics: dict[str, Any] = {
        "http_req_duration": {"values": {"p(50)": p50, "p(99)": p99}},
        "http_reqs": {"values": {"count": count}},
        "http_req_failed": {"values": {"rate": failed}},
    }
    if dropped:
        metrics["dropped_iterations"] = {"values": {"count": dropped}}
    document: dict[str, Any] = {"metrics": metrics}
    if run_ms is not None:
        document["state"] = {"testRunDurationMs": run_ms}
    return document


def conditions(
    scenario: str,
    *,
    clean: bool = True,
    step: int = 180,
    model: str | None = "lgbm-test",
    power: str | None = "ac",
) -> dict[str, Any]:
    return {
        "scenario": scenario,
        "git_commit": "0123456789abcdef",
        "git_clean": clean,
        "rule_set_version": 1,
        "shadow_model": model,
        "power": power,
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
    attempt: int = 1,
    **stack: Any,
) -> None:
    name = run.step_name(f"{scenario}-{rate:05d}rps", attempt)
    write(folder, f"{name}.json", k6)
    write(
        folder,
        f"{name}-stack.json",
        {
            "scenario": scenario,
            "rate": rate,
            "attempt": attempt,
            "summary": f"{name}.json",
            "stopped": stopped,
            **stack,
        },
    )


def stop_twice(
    folder: Path, scenario: str, rate: int, k6: dict[str, Any], stopped: str, **stack: Any
) -> None:
    """A step that passed a limit, and its repeat, which passed one too: the run stops (D61)."""
    for attempt in (1, 2):
        rate_step(folder, scenario, rate, k6, stopped, attempt, **stack)


def hold(folder: Path, scenario: str, rate: int, k6: dict[str, Any], **stack: Any) -> None:
    """A step that held, in place of whatever the folder had at that rate."""
    for path in folder.glob(f"{scenario}-{rate:05d}rps-repeat*.json"):
        path.unlink()
    rate_step(folder, scenario, rate, k6, **stack)


def hot_step(folder: Path, size: int, p99: float, stopped: str | None, attempt: int = 1) -> None:
    name = run.step_name(f"hot-{size:07d}", attempt)
    write(folder, f"{name}.json", summary(3.0, p99, 9000, 0.0))
    write(
        folder,
        f"{name}-stack.json",
        {
            "scenario": "hot",
            "rate": 50,
            "preloaded": size,
            "attempt": attempt,
            "summary": f"{name}.json",
            "members_read": {"p50": size + 4500},
            "stopped": stopped,
        },
    )


CROSSED = "p99 passed 50 ms"

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
    write(folder, "conditions-direct-off.json", conditions("direct-off", model=None))
    rate_step(folder, "direct-off", 50, summary(1.5, 5.0, 9000, 0.0), shadow_scored=0)
    rate_step(folder, "direct-off", 100, summary(1.75, 7.0, 18000, 0.0), shadow_scored=0)
    stop_twice(folder, "direct-off", 200, summary(2.0, 70.0, 36000, 0.0), CROSSED, shadow_scored=0)
    write(folder, "conditions-direct.json", conditions("direct"))
    rate_step(
        folder, "direct", 50, summary(2.0, 6.0, 9000, 0.0), decisions=9000, shadow_scored=8991
    )
    stop_twice(
        folder,
        "direct",
        100,
        summary(2.5, 61.25, 18000, 0.0),
        CROSSED,
        decisions=18000,
        shadow_scored=18000,
    )
    write(folder, "conditions-stub.json", conditions("stub"))
    rate_step(folder, "stub", 50, summary(5.0, 9.0, 9000, 0.001), **STUB_STACK)
    stop_twice(
        folder, "stub", 100, summary(6.0, 20.0, 18000, 0.02), "errors passed 1%", **STUB_STACK
    )
    write(folder, "conditions-hot.json", conditions("hot"))
    hot_step(folder, 1000, 8.0, None)
    for attempt in (1, 2):
        hot_step(folder, 10000, 75.5, CROSSED, attempt)
    return folder


def test_the_tables_state_each_rate_and_the_crossing(results: Path) -> None:
    block, problems = report.render(results)
    assert problems == []
    assert block.startswith(report.START)
    assert block.endswith(report.END)
    assert "| 50 | 9000 | 2.0 | 6.0 | 0.00% | 0 |" in block
    # A step that passed a limit, then its repeat (D61).
    assert "| 100 | 18000 | 2.5 | 61.2 | 0.00% | 0 |\n| 100, again | 18000 | 2.5 | 61.2 |" in block
    assert "p99 first passes 50 ms at **100 requests a second**." in block
    assert "held the second time" not in block


def test_a_step_that_held_when_run_again_lets_the_run_go_on(results: Path) -> None:
    """D61: direct's 100/s step passes 50 ms, its repeat holds, and the run goes on to cross,
    twice, at 200. The table shows both attempts at 100; the repeat decides, in the crossing, in
    the shadow cost and in the budget check."""
    rate_step(
        results,
        "direct",
        100,
        summary(2.4, 9.0, 18000, 0.0),
        attempt=2,
        decisions=18000,
        shadow_scored=18000,
    )
    stop_twice(results, "direct", 200, summary(3.0, 80.0, 36000, 0.0), CROSSED)
    block, problems = report.render(results)
    assert problems == []
    assert (
        "| 100 | 18000 | 2.5 | 61.2 | 0.00% | 0 |\n| 100, again | 18000 | 2.4 | 9.0 | 0.00% | 0 |"
    ) in block
    assert "p99 first passes 50 ms at **200 requests a second**." in block
    assert (
        "Run again after passing a limit, and held the second time: 100 requests a second. The "
        "run went on from there."
    ) in block
    block_2b = report.render_2b(results, Path("missing.json"))
    assert "| 100 | 1.8 | 2.4 | +0.6 | 7.0 | 9.0 | +2.0 | 100.00% |" in block_2b
    assert (
        "With it on, p99 stays within 50 ms at every rate below that: 50 to 100 requests a second."
    ) in block_2b
    assert (
        "With the model on, run again after passing a limit, and held the second time: 100 "
        "requests a second; the table shows the repeat."
    ) in block_2b


def test_a_hot_window_that_held_when_run_again_lets_the_run_go_on(results: Path) -> None:
    hot_step(results, 10000, 9.5, None, attempt=2)
    for attempt in (1, 2):
        hot_step(results, 100000, 90.0, CROSSED, attempt)
    block, problems = report.render(results)
    assert problems == []
    assert "| 10000 | 14500 | 3.0 | 75.5 | 0.00% |\n| 10000, again | 14500 | 3.0 | 9.5 |" in block
    assert "p99 first passes 50 ms with **100000 preloaded events**" in block
    assert "held the second time: 10000 preloaded events." in block


@pytest.mark.parametrize(
    ("change", "problem"),
    [
        ("cut off", "direct-00100rps stopped its run without being run again"),
        ("before D61", "direct-00100rps stopped its run without being run again"),
        ("hot", "hot-0010000 stopped its run without being run again"),
    ],
)
def test_a_stop_that_wasnt_run_again_never_goes_in_the_readme(
    results: Path, tmp_path: Path, change: str, problem: str
) -> None:
    """A run cut off between a step and its repeat, or one from before D61, which never ran
    one, didn't show that its stop was real."""
    if change == "hot":
        for path in results.glob("hot-0010000-repeat*.json"):
            path.unlink()
    else:
        for path in results.glob("direct-00100rps-repeat*.json"):
            path.unlink()
    if change == "before D61":
        stack = json.loads((results / "direct-00100rps-stack.json").read_text())
        del stack["attempt"]
        write(results, "direct-00100rps-stack.json", stack)
    _, problems = report.render(results)
    assert problems == [problem]
    assert refused(results, tmp_path)


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
    assert "| 10000, again | 14500 | 3.0 | 75.5 | 0.00% |" in block
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
    assert (
        "`direct`: Test CPU, 10 cores, 16 GiB, TestOS 1, on AC power; Docker 29.0, its VM 5 CPUs "
        "and 8.0 GiB; commit `0123456`, rule set v1, shadow model `lgbm-test`; k6 v2.3.0; steps "
        "of 180 s."
    ) in block
    assert "rule set v1, shadow off; k6 v2.3.0" in block


def test_a_host_whose_power_source_is_unknown_says_nothing_of_it(results: Path) -> None:
    """Results from before D60, or from a host without pmset, carry no power source."""
    write(results, "conditions-direct.json", conditions("direct", power=None))
    block, problems = report.render(results)
    assert "`direct`: Test CPU, 10 cores, 16 GiB, TestOS 1; Docker 29.0" in block
    assert problems == []


def test_results_from_before_shadow_scoring_read_as_off(results: Path) -> None:
    document = conditions("direct")
    del document["shadow_model"]
    write(results, "conditions-direct.json", document)
    assert "rule set v1, shadow off; k6" in report.render(results)[0]


def test_the_model_off_run_stays_out_of_the_direct_table(results: Path) -> None:
    block, _ = report.render(results)
    assert "| 200 | 36000 |" not in block
    assert "| 200, again |" not in block
    assert "| 50 | 9000 | 2.0 | 6.0 | 0.00% | 0 |" in block
    assert "| 50 | 9000 | 1.5 | 5.0 |" not in block


def stub_off(folder: Path) -> None:
    """The stub run with the model off (D61): it holds 50 and 100 and crosses, twice, at 200."""
    write(folder, "conditions-stub-off.json", conditions("stub-off", model=None))
    rate_step(folder, "stub-off", 50, summary(4.0, 8.0, 9000, 0.0), shadow_scored=0, **STUB_STACK)
    rate_step(folder, "stub-off", 100, summary(4.5, 9.5, 18000, 0.0), **STUB_STACK)
    stop_twice(folder, "stub-off", 200, summary(9.0, 300.0, 36000, 0.0), CROSSED, **STUB_STACK)


def test_the_stub_run_with_the_model_off_stays_out_of_the_stub_table(results: Path) -> None:
    stub_off(results)
    block, problems = report.render(results)
    assert problems == []
    assert "| 50 | 9000 | 5.0 | 9.0 |" in block
    assert "| 50 | 9000 | 4.0 |" not in block
    assert "| 200 |" not in block.split("#### Through the ledger stub")[1].split("#### Hot")[0]
    assert "`stub-off`: Test CPU" in block
    assert "rule set v1, shadow off; k6" in block


def test_the_2b_block_gives_the_shadow_cost_through_the_stub(results: Path) -> None:
    """D61: stub with the model off, against stub with it on, at each rate both reached. The stub
    run with it on stops for errors at 100, below the 200 where the model-off run crosses."""
    stub_off(results)
    block = report.render_2b(results, Path("missing.json"))
    stub = block.split("#### What the shadow model costs, through the ledger stub")[1]
    assert "| 50 | 4.0 | 5.0 | +1.0 | 8.0 | 9.0 | +1.0 | n/a |" in stub
    assert "| 100 | 4.5 | 6.0 | +1.5 | 9.5 | 20.0 | +10.5 | n/a |" in stub
    assert "| 200 |" not in stub
    # 100's p99 is within 50 ms, but its errors passed 1%: it didn't hold, so it isn't within
    # budget.
    assert (
        "With the model off, p99 first passes 50 ms at **200 transfers a second**. With it on, "
        "the run stopped at 100 transfers a second because errors passed 1%, below that rate."
    ) in stub
    # The direct table comes first and is unchanged.
    direct = block.split("#### What the shadow model costs, straight to the decision API")[1]
    assert direct.index("| 50 | 1.5 | 2.0 |") < direct.index("through the ledger stub")


# --- The 2b block (D57.2, D57.4; Phase 8, criteria 3 and 4) -------------------------------------


def backtest_file(folder: Path) -> Path:
    def line(
        who: str, rate: str, threshold: float | None, held: int, caught: int, side: str = "exact"
    ) -> Line:
        return Line(
            who,
            rate,
            side,
            0 if side == "exact" else 12,
            threshold,
            AtThreshold(
                transfers=1000,
                held=held,
                fraud=40,
                fraud_held=caught,
                fraud_cents=4_000_000,
                fraud_cents_held=caught * 100_000,
                not_fraud=960,
                not_fraud_held=held - caught,
            ),
        )

    result = Result(
        csv_name="paysim.csv",
        csv_sha256="ab" * 32,
        rules_file="ml/rules/paysim.yaml",
        rules_version=2,
        rules_sha256="cd" * 32,
        model_name="lgbm-test",
        test_days=(24, 30),
        lines=(
            line("rules", "own", None, 20, 4),
            line("model", "rules", 0.4, 20, 9),
            line("model", "1%", 0.6, 4, 3, "above"),
            line("model", "1%", 0.5, 16, 8, "with"),
        ),
    )
    path = folder / "backtest.json"
    path.write_text(json.dumps(data(result)))
    return path


def no_backtest(tmp_path: Path, results: Path) -> list[str]:
    """Arguments that point the report at these results and at no backtest, so a test never
    reads the repo's own files."""
    return ["--results", str(results), "--backtest", str(tmp_path / "no-backtest.json")]


def test_a_backtest_file_in_another_shape_is_refused(results: Path, tmp_path: Path) -> None:
    readme = tmp_path / "README.md"
    blocks = f"{report.START}\n{report.END}\n{report.START_2B}\n{report.END_2B}\n"
    readme.write_text(blocks)
    stale = tmp_path / "backtest.json"
    stale.write_text(json.dumps({"lines": [{"who": "rules"}]}))
    args = ["--results", str(results), "--backtest", str(stale), "--write", str(readme)]
    assert report.main(args) == 1
    assert readme.read_text() == blocks


def test_the_2b_block_holds_the_backtest_and_the_shadow_cost(results: Path, tmp_path: Path) -> None:
    block = report.render_2b(results, backtest_file(tmp_path))
    assert block.startswith(report.START_2B)
    assert block.endswith(report.END_2B)
    assert "Days 24 to 30 of `paysim.csv`: 1000 transfers, of which fraud 40" in block
    assert (
        "| PaySim rules (v2), at their own hold rate | 2.00% (20) | 10.00% (4 of 40) | 10.00% "
        "| 1.67% (16) |"
    ) in block
    assert "| model `lgbm-test`, at the rules' hold rate | 2.00% (20) | 22.50% (9 of 40) |" in block
    assert "| model `lgbm-test`, at 1%: above a tie of 12 | 0.40% (4) | 7.50% (3 of 40) |" in block
    assert (
        "| model `lgbm-test`, at 1%: with the tie of 12 | 1.60% (16) | 20.00% (8 of 40) |" in block
    )
    # Added latency is on minus off, at each rate both runs reached; Scored is the on run's.
    assert "| 50 | 1.5 | 2.0 | +0.5 | 5.0 | 6.0 | +1.0 | 99.90% |" in block
    # p50 off is 1.75, shown as 1.8: the added p50 is 2.5 - 1.8 as shown, not 0.75 rounded up.
    assert "| 100 | 1.8 | 2.5 | +0.7 | 7.0 | 61.2 | +54.2 | 100.00% |" in block
    assert "| 200 |" not in block


def test_the_budget_check_names_a_crossing_below_the_model_off_one(results: Path) -> None:
    """Off crosses at 200; on crosses at 100, below it: criterion 4 fails, and says where."""
    block = report.render_2b(results, Path("missing.json"))
    assert (
        "With the model off, p99 first passes 50 ms at **200 requests a second**. With it on, "
        "p99 passes 50 ms at 100 requests a second, below that rate."
    ) in block
    assert "No backtest yet: run `make backtest`." in block


def test_the_budget_check_passes_when_p99_holds_below_the_crossing(results: Path) -> None:
    hold(
        results,
        "direct",
        100,
        summary(2.5, 9.0, 18000, 0.0),
        decisions=18000,
        shadow_scored=18000,
    )
    stop_twice(results, "direct", 200, summary(3.0, 80.0, 36000, 0.0), CROSSED)
    block = report.render_2b(results, Path("missing.json"))
    assert (
        "With it on, p99 stays within 50 ms at every rate below that: 50 to 100 requests a "
        "second." in block
    )


def test_the_budget_check_says_when_a_rate_below_wasnt_measured(results: Path) -> None:
    """The model-on run stops at 100 for errors, while the model-off run only crosses at 400: 200
    was never measured with the model on, so the check can't pass."""
    stop_twice(results, "direct", 100, summary(2.5, 9.0, 18000, 0.02), "errors passed 1%")
    stop_twice(results, "direct-off", 400, summary(2.0, 80.0, 72000, 0.0), CROSSED)
    hold(results, "direct-off", 200, summary(2.0, 9.0, 36000, 0.0))
    block = report.render_2b(results, Path("missing.json"))
    assert (
        "With it on, p99 wasn't measured at 200 requests a second: The run stopped at 100 "
        "requests a second because errors passed 1%" in block
    )
    assert "With it on, p99 stays within 50 ms" not in block


def test_without_both_runs_the_2b_block_says_so(results: Path) -> None:
    (results / "conditions-direct-off.json").unlink()
    block = report.render_2b(results, Path("missing.json"))
    assert "No run with the model off and on in these results: run `make bench`." in block


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
    assert report.main([*no_backtest(tmp_path, results), "--write", str(readme)]) == 1
    assert readme.read_text() == f"intro\n{report.START}\n{report.END}\n"
    _, problems = report.render(results)
    assert any(problem in p for p in problems)


def refused(results: Path, tmp_path: Path) -> bool:
    """Whether `make report` refuses these results, leaving README.md as it was."""
    readme = tmp_path / "README.md"
    blocks = f"{report.START}\n{report.END}\n{report.START_2B}\n{report.END_2B}\n"
    readme.write_text(blocks)
    code = report.main([*no_backtest(tmp_path, results), "--write", str(readme)])
    return code == 1 and readme.read_text() == blocks


@pytest.mark.parametrize(
    ("run_ms", "problem"),
    [
        # The first 2026-11-08 run: the host slept for 22 minutes inside an 800/s step (D60).
        (1_480_761.753, "direct-00050rps ran for 1480.8 s, not its 180 s step"),
        (189_500.0, "direct-00050rps ran for 189.5 s, not its 180 s step"),
        (170_000.0, "direct-00050rps ran for 170.0 s, not its 180 s step"),
        (None, "direct-00050rps's k6 summary doesn't say how long it ran"),
    ],
    ids=["slept", "just over 5% long", "short", "no run time"],
)
def test_a_step_that_ran_off_its_length_never_goes_in_the_readme(
    results: Path, tmp_path: Path, run_ms: float | None, problem: str
) -> None:
    k6 = summary(2.0, 6.0, 9000, 0.0, run_ms=run_ms)
    rate_step(results, "direct", 50, k6, decisions=9000, shadow_scored=8991)
    _, problems = report.render(results)
    assert problems == [problem]
    assert refused(results, tmp_path)


def test_a_step_a_little_long_while_k6_waits_for_requests_in_flight_is_accepted(
    results: Path,
) -> None:
    """An overloaded step runs on while k6 waits for its last requests: 182.3 s, as the stub's
    800/s step did on 2026-11-03, is within 5%."""
    k6 = summary(5.0, 9.0, 9000, 0.001, run_ms=182_273.53)
    rate_step(results, "stub", 50, k6, **STUB_STACK)
    _, problems = report.render(results)
    assert problems == []


@pytest.mark.parametrize(
    ("change", "problem"),
    [
        ({"step_power": {"start": "ac", "end": "battery"}}, "direct-00050rps ran on battery"),
        ({"step_power": {"start": "battery", "end": "ac"}}, "direct-00050rps ran on battery"),
        ({"scenario_power": "battery"}, "direct started on battery"),
    ],
    ids=["unplugged during a step", "plugged in during a step", "scenario"],
)
def test_results_from_a_host_on_battery_never_go_in_the_readme(
    results: Path, tmp_path: Path, change: dict[str, Any], problem: str
) -> None:
    if "step_power" in change:
        k6 = summary(2.0, 6.0, 9000, 0.0)
        power = change["step_power"]
        rate_step(results, "direct", 50, k6, decisions=9000, shadow_scored=8991, power=power)
    else:
        write(results, "conditions-direct.json", conditions("direct", power="battery"))
    _, problems = report.render(results)
    assert problems == [problem]
    assert refused(results, tmp_path)


@pytest.mark.parametrize(
    ("pmset", "power"),
    [
        (
            "Now drawing from 'AC Power'\n -InternalBattery-0 (id=1)\t51%; AC attached; not "
            "charging present: true",
            "ac",
        ),
        (
            "Now drawing from 'Battery Power'\n -InternalBattery-0 (id=1)\t70%; discharging; "
            "(no estimate) present: true",
            "battery",
        ),
        ("", None),  # no pmset: not macOS, or the command failed
        ("Now drawing from 'UPS Power'\n", None),
    ],
    ids=["ac", "battery", "no pmset", "something else"],
)
def test_the_power_source_is_read_from_pmset(pmset: str, power: str | None) -> None:
    assert run.power_from(pmset) == power


def test_the_bench_wont_start_on_battery(monkeypatch: pytest.MonkeyPatch) -> None:
    """It refuses before it touches Docker or writes a results folder."""
    monkeypatch.setattr(run, "power_source", lambda: "battery")

    def no_docker() -> bool:
        raise AssertionError("the bench went on past the power check")

    monkeypatch.setattr(run, "normal_stack_running", no_docker)
    assert run.main([]) == 2


# --- Running a step again (D61) -----------------------------------------------------------------


@pytest.mark.parametrize(
    ("reasons", "attempts", "stop"),
    [
        ([None], [1], None),
        ([CROSSED, None], [1, 2], None),
        ([CROSSED, "errors passed 1%"], [1, 2], "errors passed 1%"),
    ],
    ids=["held", "held when run again", "passed a limit twice"],
)
def test_a_step_that_passes_a_limit_runs_once_more(
    reasons: list[str | None], attempts: list[int], stop: str | None
) -> None:
    ran: list[int] = []

    def measure(attempt: int) -> str | None:
        ran.append(attempt)
        return reasons[attempt - 1]

    assert run.confirmed(measure, "direct at 800/s") == stop
    assert ran == attempts


def test_a_rate_scenario_stops_only_where_both_attempts_pass_a_limit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A stall fails 100/s once and the run goes on; 200/s fails twice, and the run stops there."""
    reasons = {(100, 1): CROSSED, (200, 1): CROSSED, (200, 2): "errors passed 1%"}
    ran: list[tuple[int, int]] = []

    def step(out: Path, scenario: str, rate: int, step_s: int, attempt: int) -> str | None:
        assert (out, scenario, step_s) == (tmp_path, "stub-off", 180)
        ran.append((rate, attempt))
        return reasons.get((rate, attempt))

    monkeypatch.setattr(run, "rate_step", step)
    run.rate_steps(tmp_path, "stub-off", 180)
    assert ran == [(50, 1), (100, 1), (100, 2), (200, 1), (200, 2)]


def test_the_hot_scenario_stops_only_where_both_attempts_pass_50_ms(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    reasons = {(10_000, 1): CROSSED, (100_000, 1): CROSSED, (100_000, 2): CROSSED}
    preloaded: list[int] = []
    ran: list[tuple[int, int]] = []

    def step(out: Path, hot: uuid.UUID, size: int, step_s: int, attempt: int) -> str | None:
        ran.append((size, attempt))
        return reasons.get((size, attempt))

    monkeypatch.setattr(run, "preload", lambda hot, start, total: preloaded.append(total))
    monkeypatch.setattr(run, "hot_step", step)
    run.hot_steps(tmp_path, 180)
    assert preloaded == [1_000, 10_000, 100_000]
    assert ran == [(1_000, 1), (10_000, 1), (10_000, 2), (100_000, 1), (100_000, 2)]


def test_the_session_runs_every_scenario_with_its_shadow_setting(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """D57.2 and D61: direct and stub each with the model off and then on, then hot with it on,
    each on a fresh stack."""
    ran: list[tuple[str, ...]] = []
    monkeypatch.setattr(run, "ROOT", tmp_path)
    monkeypatch.setattr(run, "RESULTS", tmp_path / "bench" / "results")
    monkeypatch.setattr(run, "power_source", lambda: "ac")
    monkeypatch.setattr(run, "normal_stack_running", lambda: False)
    monkeypatch.setattr(run, "stack_up", lambda shadow: ran.append(("up", f"shadow={shadow}")))
    monkeypatch.setattr(run, "stack_down", lambda: ran.append(("down",)))
    monkeypatch.setattr(run, "conditions", lambda scenario, step_s, shadow: {})
    monkeypatch.setattr(run, "rate_steps", lambda out, scenario, step_s: ran.append((scenario,)))
    monkeypatch.setattr(run, "hot_steps", lambda out, step_s: ran.append(("hot",)))
    assert run.main([]) == 0
    expected: list[tuple[str, ...]] = []
    for scenario, shadow in (
        ("direct-off", False),
        ("direct", True),
        ("stub-off", False),
        ("stub", True),
        ("hot", True),
    ):
        expected += [("up", f"shadow={shadow}"), (scenario,), ("down",)]
    assert ran == expected


@pytest.mark.parametrize(
    ("scenario", "target", "url"),
    [
        ("direct-off", "decisions", run.API_URL),
        ("stub-off", "transfers", run.STUB_URL),
        ("stub", "transfers", run.STUB_URL),
    ],
)
def test_a_steps_files_are_read_back_by_the_report(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    scenario: str,
    target: str,
    url: str,
) -> None:
    """The runner's files for a step and its repeat, as the report reads them: both attempts, the
    repeat after the first, and the stub scenarios sent to the stub."""
    now = run.utc_now()

    def k6_step(
        out: Path, name: str, to: str, base_url: str, rate: int, step_s: int, **extra: str
    ) -> tuple[dict[str, Any], Any, Any]:
        assert (to, base_url) == (target, url)
        k6 = summary(2.0, 70.0 if name.endswith("repeat") else 60.0, rate * step_s, 0.0)
        write(out, f"{name}.json", k6)
        return k6, now, now

    monkeypatch.setattr(run, "k6_step", k6_step)
    monkeypatch.setattr(run, "power_source", lambda: "ac")
    monkeypatch.setattr(run, "counter_total", lambda base_url, name: 0.0)
    monkeypatch.setattr(run, "decisions_between", lambda start, end: [])
    monkeypatch.setattr(run, "consumer_offset_lag", lambda: 0)
    assert run.rate_step(tmp_path, scenario, 800, 180, 1) == CROSSED
    assert run.rate_step(tmp_path, scenario, 800, 180, 2) == CROSSED
    rows = report.steps(tmp_path, scenario)
    assert [(r["rate"], report.attempt(r), r["summary"]) for r in rows] == [
        (800, 1, f"{scenario}-00800rps.json"),
        (800, 2, f"{scenario}-00800rps-repeat.json"),
    ]
    assert report.p50_p99(report.deciding(rows)[0]) == (2.0, 70.0)
    assert report.unconfirmed(rows, "rate") == []
    assert rows[1]["power"] == {"start": "ac", "end": "ac"}


def test_check_passes_after_write_and_fails_after_an_edit(results: Path, tmp_path: Path) -> None:
    readme = tmp_path / "README.md"
    readme.write_text(
        f"# Title\n\n{report.START}\nold\n{report.END}\n\nmiddle\n\n"
        f"{report.START_2B}\nold\n{report.END_2B}\n\nafter\n"
    )
    paths = ["--results", str(results), "--backtest", str(backtest_file(tmp_path))]
    assert report.main([*paths, "--write", str(readme)]) == 0
    text = readme.read_text()
    assert text.startswith("# Title\n\n")
    assert "\n\nmiddle\n\n" in text
    assert text.endswith("\n\nafter\n")
    assert "| 9 of 40" not in text.split(report.START_2B)[0]
    assert "22.50% (9 of 40)" in text.split(report.START_2B)[1]
    assert report.main([*paths, "--check", str(readme)]) == 0

    for edit in (("| 2.0 | 6.0 |", "| 2.0 | 5.0 |"), ("22.50%", "25.00%")):
        readme.write_text(text.replace(*edit))
        assert report.main([*paths, "--check", str(readme)]) == 1


@pytest.mark.parametrize(
    "text",
    [
        "no markers\n",
        f"{report.START}\n{report.END}\n",
        f"{report.START}\n{report.END}\n{report.START_2B}\n{report.END_2B}\n{report.START_2B}\n",
    ],
    ids=["none", "no 2b block", "two 2b starts"],
)
def test_a_readme_without_exactly_one_of_each_block_is_refused(
    results: Path, tmp_path: Path, text: str
) -> None:
    readme = tmp_path / "README.md"
    readme.write_text(text)
    assert report.main([*no_backtest(tmp_path, results), "--check", str(readme)]) == 1


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
