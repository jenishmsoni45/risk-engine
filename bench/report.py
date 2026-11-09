"""Turns the files `make bench` and `make backtest` write into README.md's tables (design.md
section 12, D29, D57.4).

    uv run --locked python -m bench.report                    print the blocks
    uv run --locked python -m bench.report --write README.md  replace README.md's blocks
    uv run --locked python -m bench.report --check README.md  exit 1 if a block differs

It reads the newest folder under bench/results/ (or --results) and ml/reports/backtest.json (or
--backtest), and writes two blocks:

- the bench block: per scenario, p50 and p99 at each stated rate, the first rate whose p99 passes
  50 ms (or, when errors or the arrival rate gave out first, the highest rate held), the stub's
  timeout rate, the feature lag and the consumer's offset lag, and the hot account's p99 against
  its window size;
- the 2b block: the backtest's comparison, and what the shadow model costs, straight to the
  decision API and through the stub: p50 and p99 with it off and on at each rate, the difference,
  the share of decisions it scored, and whether p99 stayed within 50 ms with it on at every rate
  below the one where it crossed with it off (Phase 8, criterion 4, on the direct path).

A step that passed a limit was run once more (D61). The tables show both attempts, and the repeat
decides: the run stopped at a rate only if the repeat passed a limit too, and the shadow cost
compares the repeats.

Every number in either block comes from those files. Results from a dirty tree, from shortened
steps, from a step k6 ran for more than 5% longer or shorter than its length, from a host on
battery, or from a scenario that stopped without running its last step again are printed but
never written to, or accepted in, README.md (D60, D61).
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from ml.backtest import comparison, from_data

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "bench" / "results"
BACKTEST = ROOT / "ml" / "reports" / "backtest.json"
START = "<!-- bench:start -->"
END = "<!-- bench:end -->"
START_2B = "<!-- 2b:start -->"
END_2B = "<!-- 2b:end -->"
PLAN_STEP_SECONDS = 180  # D40o
# How far k6's own run time may stray from the step (D60). An overloaded step runs a little long
# while k6 waits for requests still in flight; a host that sleeps runs it far longer.
RUN_TIME_SLACK = 0.05
POWER = {"ac": "on AC power", "battery": "on battery"}
CROSSED = "p99 passed 50 ms"


class ReportError(Exception):
    pass


def latest(results: Path) -> Path:
    folders = sorted(p for p in results.iterdir() if p.is_dir() and any(p.glob("conditions-*")))
    if not folders:
        raise ReportError(f"no results under {results}: run `make bench`")
    return folders[-1]


def load(path: Path) -> dict[str, Any]:
    document: dict[str, Any] = json.loads(path.read_text())
    return document


def steps(folder: Path, scenario: str) -> list[dict[str, Any]]:
    found = []
    for path in folder.glob(f"{scenario}-*-stack.json"):
        step = load(path)
        if step["scenario"] != scenario:  # direct-* also matches direct-off's files
            continue
        summary = load(folder / step["summary"])
        step["k6"] = summary["metrics"]
        step["k6_run_ms"] = summary.get("state", {}).get("testRunDurationMs")
        found.append(step)
    key = size_key(scenario)
    return sorted(found, key=lambda s: (int(s[key]), attempt(s)))


def size_key(scenario: str) -> str:
    """What a scenario's steps grow: the rate, or the hot account's window."""
    return "preloaded" if scenario == "hot" else "rate"


def attempt(step: dict[str, Any]) -> int:
    """1, or 2 for the repeat of a step that passed a limit (D61); results from before D61 hold
    first attempts only."""
    return int(step.get("attempt", 1))


def deciding(rows: list[dict[str, Any]], key: str = "rate") -> list[dict[str, Any]]:
    """One step per rate or window size, from rows sorted by it and then by attempt: the repeat
    where there is one, since it decides whether the run stopped there (D61)."""
    last: dict[int, dict[str, Any]] = {}
    for row in rows:
        last[row[key]] = row
    return list(last.values())


def label(step: dict[str, Any], key: str = "rate") -> str:
    """A table's first cell: the rate or window size, and whether this is the repeat."""
    return f"{step[key]}, again" if attempt(step) == 2 else str(step[key])


def listing(values: list[int]) -> str:
    """200; 200 and 400; 200, 400 and 800."""
    words = [str(v) for v in values]
    return words[0] if len(words) == 1 else f"{', '.join(words[:-1])} and {words[-1]}"


def held_on_repeat(rows: list[dict[str, Any]], key: str = "rate") -> list[int]:
    """The rates or sizes whose first attempt passed a limit and whose repeat held (D61)."""
    return [r[key] for r in rows if attempt(r) == 2 and r.get("stopped") is None]


def repeat_note(rows: list[dict[str, Any]], unit: str, key: str = "rate") -> list[str]:
    held = held_on_repeat(rows, key)
    if not held:
        return []
    return [
        f"Run again after passing a limit, and held the second time: {listing(held)} {unit}. "
        "The run went on from there."
    ]


def ms(value: float) -> str:
    return f"{value:.1f}"


def percent(value: float) -> str:
    return f"{100 * value:.2f}%"


def p50_p99(step: dict[str, Any]) -> tuple[float, float]:
    values = step["k6"]["http_req_duration"]["values"]
    return values["p(50)"], values["p(99)"]


def latency(step: dict[str, Any]) -> tuple[str, str]:
    p50, p99 = p50_p99(step)
    return ms(p50), ms(p99)


def requests(step: dict[str, Any]) -> int:
    return int(step["k6"]["http_reqs"]["values"]["count"])


def errors(step: dict[str, Any]) -> str:
    return percent(step["k6"]["http_req_failed"]["values"]["rate"])


def dropped(step: dict[str, Any]) -> int:
    return int(step["k6"].get("dropped_iterations", {}).get("values", {}).get("count", 0))


def crossing(rows: list[dict[str, Any]], unit: str) -> str:
    """The sentence under a rate table (D40o), from the steps that decided each rate (D61)."""
    rows = deciding(rows)
    if not rows:
        return "No steps were run."
    last = rows[-1]
    held = [r for r in rows if r.get("stopped") is None]
    if last.get("stopped") == CROSSED:
        return f"p99 first passes 50 ms at **{last['rate']} {unit}**."
    highest = f"{held[-1]['rate']} {unit}" if held else "none"
    if last.get("stopped") is None:
        return f"p99 stayed within 50 ms at every rate run; the highest was {highest}."
    return (
        f"The run stopped at {last['rate']} {unit} because {last['stopped']}, before p99 passed "
        f"50 ms; the highest rate held was {highest}."
    )


def direct_table(rows: list[dict[str, Any]]) -> list[str]:
    lines = [
        "| Rate (req/s) | Requests | p50 (ms) | p99 (ms) | Errors | Dropped |",
        "|---:|---:|---:|---:|---:|---:|",
    ]
    for r in rows:
        p50, p99 = latency(r)
        lines.append(f"| {label(r)} | {requests(r)} | {p50} | {p99} | {errors(r)} | {dropped(r)} |")
    unit = "requests a second"
    return [*lines, "", crossing(rows, unit), *repeat_note(rows, unit)]


def stub_table(rows: list[dict[str, Any]]) -> list[str]:
    lines = [
        "| Rate (req/s) | Requests | p50 (ms) | p99 (ms) | Errors | Timeout rate "
        "| Feature lag p50 (ms) | Feature lag p99 (ms) | Offset lag at end |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for r in rows:
        p50, p99 = latency(r)
        timeouts = r["stub_backup_answers"] / r["stub_transfers"] if r["stub_transfers"] else 0.0
        lag = r["feature_lag_ms"]
        lag50 = "n/a" if lag["p50"] is None else str(lag["p50"])
        lag99 = "n/a" if lag["p99"] is None else str(lag["p99"])
        lines.append(
            f"| {label(r)} | {requests(r)} | {p50} | {p99} | {errors(r)} | {percent(timeouts)} "
            f"| {lag50} | {lag99} | {r['consumer_offset_lag_at_end']} |"
        )
    unit = "transfers a second"
    return [*lines, "", crossing(rows, unit), *repeat_note(rows, unit)]


def hot_table(rows: list[dict[str, Any]]) -> list[str]:
    lines = [
        "| Preloaded events | Members read, median | p50 (ms) | p99 (ms) | Errors |",
        "|---:|---:|---:|---:|---:|",
    ]
    for r in rows:
        p50, p99 = latency(r)
        lines.append(
            f"| {label(r, 'preloaded')} | {r['members_read']['p50']} | {p50} | {p99} "
            f"| {errors(r)} |"
        )
    over = [r for r in deciding(rows, "preloaded") if r.get("stopped")]
    sentence = (
        f"p99 first passes 50 ms with **{over[0]['preloaded']} preloaded events** in the "
        "receiver's 24 h window."
        if over
        else "p99 stayed within 50 ms at every window size run."
    )
    return [*lines, "", sentence, *repeat_note(rows, "preloaded events", "preloaded")]


def scored(step: dict[str, Any]) -> str:
    """The share of the step's decisions that hold a shadow score."""
    if "shadow_scored" not in step or not step.get("decisions"):
        return "n/a"
    return percent(step["shadow_scored"] / step["decisions"])


def added(on: float, off: float) -> str:
    """On minus off, from the values the table shows, so that a reader subtracting them gets the
    same number."""
    return f"{float(ms(on)) - float(ms(off)):+.1f}"


def shadow_cost_table(
    off: list[dict[str, Any]], on: list[dict[str, Any]], unit: str = "requests a second"
) -> list[str]:
    """p50 and p99 with the model off and on at each rate both runs reached, and the difference
    (D57.2), from the step that decided each rate (D61); then whether p99 stayed within 50 ms with
    it on below the rate where it crossed with it off (Phase 8, criterion 4)."""
    lines = [
        "| Rate (req/s) | p50 off (ms) | p50 on (ms) | Added p50 (ms) | p99 off (ms) "
        "| p99 on (ms) | Added p99 (ms) | Scored |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    off, on = deciding(off), deciding(on)
    by_rate = {r["rate"]: r for r in on}
    for r in off:
        if r["rate"] not in by_rate:
            continue
        p50_off, p99_off = p50_p99(r)
        p50_on, p99_on = p50_p99(by_rate[r["rate"]])
        lines.append(
            f"| {r['rate']} | {ms(p50_off)} | {ms(p50_on)} | {added(p50_on, p50_off)} "
            f"| {ms(p99_off)} | {ms(p99_on)} | {added(p99_on, p99_off)} "
            f"| {scored(by_rate[r['rate']])} |"
        )
    notes = []
    for side, rows in (("off", off), ("on", on)):
        held = held_on_repeat(rows)
        if held:
            notes.append(
                f"With the model {side}, run again after passing a limit, and held the second "
                f"time: {listing(held)} {unit}; the table shows the repeat."
            )
    return [*lines, "", *budget_sentences(off, on, unit), *notes]


def budget_sentences(
    off: list[dict[str, Any]], on: list[dict[str, Any]], unit: str = "requests a second"
) -> list[str]:
    off, on = deciding(off), deciding(on)
    crossed = [r["rate"] for r in off if r.get("stopped") == CROSSED]
    if crossed:
        first = f"With the model off, p99 first passes 50 ms at **{crossed[0]} {unit}**."
        scope = "every rate below that"
        needed = [r["rate"] for r in off if r["rate"] < crossed[0]]
    else:
        first = f"With the model off, p99 never passed 50 ms. {crossing(off, unit)}"
        scope = "every rate the model-off run held"
        needed = [r["rate"] for r in off if r.get("stopped") is None]
    measured = {r["rate"]: r for r in on}
    missing = [rate for rate in needed if rate not in measured]
    # A step within budget is one that held: its p99 within 50 ms, and its errors and dropped
    # requests within 1%.
    over = [rate for rate in needed if rate in measured and measured[rate].get("stopped")]
    if not needed:
        second = "No rate below it was run, so there is nothing to compare."
    elif missing:
        second = (
            f"With it on, p99 wasn't measured at {', '.join(map(str, missing))} {unit}: "
            f"{crossing(on, unit)}"
        )
    elif over and measured[over[0]]["stopped"] == CROSSED:
        second = f"With it on, p99 passes 50 ms at {over[0]} {unit}, below that rate."
    elif over:
        second = (
            f"With it on, the run stopped at {over[0]} {unit} because "
            f"{measured[over[0]]['stopped']}, below that rate."
        )
    else:
        span = f"{needed[0]} to {needed[-1]}" if len(needed) > 1 else str(needed[0])
        second = f"With it on, p99 stays within 50 ms at {scope}: {span} {unit}."
    return [f"{first} {second}"]


def conditions_line(c: dict[str, Any]) -> str:
    host, docker = c["host"], c["docker"]
    gib = 1024**3
    model = c.get("shadow_model")
    shadow = f"shadow model `{model}`" if model else "shadow off"
    power = f", {POWER[c['power']]}" if c.get("power") in POWER else ""
    return (
        f"`{c['scenario']}`: {host['cpu']}, {host['cpus']} cores, "
        f"{host['memory_bytes'] / gib:.0f} GiB, {host['os']}{power}; Docker {docker['version']}, "
        f"its VM {docker['vm_cpus']} CPUs and {docker['vm_memory_bytes'] / gib:.1f} GiB; commit "
        f"`{c['git_commit'][:7]}`, rule set v{c['rule_set_version']}, {shadow}; "
        f"{c['k6'].split(' (')[0]}; steps of {c['procedure']['step_seconds']} s."
    )


def _relative(path: Path) -> Path:
    return path.relative_to(ROOT) if path.is_relative_to(ROOT) else path


def _conditions(folder: Path) -> dict[str, dict[str, Any]]:
    return {path.stem.split("-", 1)[1]: load(path) for path in folder.glob("conditions-*")}


def step_problems(step: dict[str, Any], step_s: int) -> list[str]:
    """Why a step's numbers can't be trusted (D60): k6 ran it for more than 5% longer or shorter
    than its step, as when the host sleeps, or the host was on battery at its start or end."""
    name = step["summary"].removesuffix(".json")
    problems = []
    ran_ms = step.get("k6_run_ms")
    if ran_ms is None:
        problems.append(f"{name}'s k6 summary doesn't say how long it ran")
    elif abs(ran_ms - step_s * 1000) > RUN_TIME_SLACK * step_s * 1000:
        problems.append(f"{name} ran for {ran_ms / 1000:.1f} s, not its {step_s} s step")
    if "battery" in (step.get("power") or {}).values():
        problems.append(f"{name} ran on battery")
    return problems


def unconfirmed(rows: list[dict[str, Any]], key: str) -> list[str]:
    """Steps that stopped their run without being run again (D61), as results from before D61
    or from a run cut off between the two attempts do."""
    return [
        f"{r['summary'].removesuffix('.json')} stopped its run without being run again"
        for r in deciding(rows, key)
        if r.get("stopped") is not None and attempt(r) == 1
    ]


def render(folder: Path) -> tuple[str, list[str]]:
    """The README bench block for a results folder, and why it can't go in README.md (if it
    can't)."""
    problems = []
    conditions = _conditions(folder)
    for scenario, c in sorted(conditions.items()):
        if not c["git_clean"]:
            problems.append(f"{scenario} ran from a tree with uncommitted changes")
        if c["procedure"]["step_seconds"] != PLAN_STEP_SECONDS:
            problems.append(f"{scenario}'s steps were {c['procedure']['step_seconds']} s")
        if c.get("power") == "battery":
            problems.append(f"{scenario} started on battery")
        found = steps(folder, scenario)
        for step in found:
            problems += step_problems(step, c["procedure"]["step_seconds"])
        problems += unconfirmed(found, size_key(scenario))
    out = [
        START,
        f"<!-- Made by `make report` from `{_relative(folder)}/`; `make check-readme` fails if "
        "it differs. Don't edit by hand. -->",
        "",
    ]
    sections: tuple[tuple[str, str, Callable[[list[dict[str, Any]]], list[str]]], ...] = (
        ("direct", "Straight to the decision API", direct_table),
        ("stub", "Through the ledger stub (decision call, posting, relay, consumer)", stub_table),
        ("hot", "Hot account: one receiver, 50 requests a second", hot_table),
    )
    for scenario, title, table in sections:
        if scenario in conditions:
            out += [f"#### {title}", "", *table(steps(folder, scenario)), ""]
    out += ["Measured on:", ""]
    out += [f"- {conditions_line(c)}" for _, c in sorted(conditions.items())]
    out += ["", END]
    return "\n".join(out), problems


def render_2b(folder: Path, backtest: Path) -> str:
    """The README 2b block: the backtest's comparison and the shadow model's cost."""
    out = [
        START_2B,
        f"<!-- Made by `make report` from `{_relative(backtest)}` and `{_relative(folder)}/`; "
        "`make check-readme` fails if it differs. Don't edit by hand. -->",
        "",
        "#### The rules against the model, on PaySim's test days",
        "",
    ]
    if backtest.is_file():
        try:
            result = from_data(load(backtest))
        except (KeyError, TypeError, ValueError) as exc:
            raise ReportError(f"{backtest} can't be read, so run `make backtest`: {exc}") from exc
        first = result.lines[0].counts
        out += [
            f"Days {result.test_days[0]} to {result.test_days[1]} of `{result.csv_name}`: "
            f"{first.transfers} transfers, of which fraud {first.fraud}, seen by neither side "
            "(D38).",
            "",
            *comparison(result),
        ]
    else:
        out.append("No backtest yet: run `make backtest`.")
    conditions = _conditions(folder)
    paths = (
        ("direct", "straight to the decision API", "requests a second"),
        ("stub", "through the ledger stub", "transfers a second"),
    )
    for on, where, unit in paths:
        off = f"{on}-off"
        out += ["", f"#### What the shadow model costs, {where}", ""]
        if off in conditions and on in conditions:
            out += shadow_cost_table(steps(folder, off), steps(folder, on), unit)
        else:
            out.append("No run with the model off and on in these results: run `make bench`.")
    out += ["", END_2B]
    return "\n".join(out)


def replace_block(readme: str, block: str, start: str = START, end: str = END) -> str:
    if readme.count(start) != 1 or readme.count(end) != 1:
        raise ReportError(f"README.md must hold one {start} and one {end}")
    before, rest = readme.split(start)
    _, after = rest.split(end)
    return before + block + after


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--results", type=Path, help="a results folder; default: the newest")
    parser.add_argument("--backtest", type=Path, default=BACKTEST, help="backtest.json")
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--write", type=Path, metavar="README")
    action.add_argument("--check", type=Path, metavar="README")
    args = parser.parse_args(argv)
    try:
        folder = (args.results or latest(RESULTS)).resolve()
        block, problems = render(folder)
        block_2b = render_2b(folder, args.backtest.resolve())
        if args.write is None and args.check is None:
            print(block)
            print()
            print(block_2b)
            for problem in problems:
                print(f"not for README.md: {problem}", file=sys.stderr)
            return 0
        if problems:
            raise ReportError("these results can't go in README.md: " + "; ".join(problems))
        target: Path = args.write or args.check
        current = target.read_text()
        wanted = replace_block(current, block)
        wanted = replace_block(wanted, block_2b, START_2B, END_2B)
        if args.write is not None:
            target.write_text(wanted)
            return 0
        if current != wanted:
            print(
                f"{target}'s generated blocks differ from `make report`'s output for {folder.name}",
                file=sys.stderr,
            )
            return 1
        return 0
    except ReportError as exc:
        print(exc, file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
