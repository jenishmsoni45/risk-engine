"""Turns the files `make bench` writes into README.md's tables (design.md section 12, D29).

    uv run --locked python -m bench.report                    print the tables
    uv run --locked python -m bench.report --write README.md  replace README.md's bench block
    uv run --locked python -m bench.report --check README.md  exit 1 if that block differs

It reads the newest folder under bench/results/ (or --results), and reports, per scenario, p50
and p99 at each stated rate, the first rate whose p99 passes 50 ms (or, when errors or the
arrival rate gave out first, the highest rate held), the stub's timeout rate, the feature lag
and the consumer's offset lag, and the hot account's p99 against its window size. Every number in
the block comes from those files. Results from a dirty tree or from shortened steps are printed
but never written to, or accepted in, README.md.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "bench" / "results"
START = "<!-- bench:start -->"
END = "<!-- bench:end -->"
PLAN_STEP_SECONDS = 180  # D40o
BUDGET_MS = 50.0


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
        step["k6"] = load(folder / step["summary"])["metrics"]
        found.append(step)
    key = "preloaded" if scenario == "hot" else "rate"
    return sorted(found, key=lambda s: int(s[key]))


def ms(value: float) -> str:
    return f"{value:.1f}"


def percent(value: float) -> str:
    return f"{100 * value:.2f}%"


def latency(step: dict[str, Any]) -> tuple[str, str]:
    values = step["k6"]["http_req_duration"]["values"]
    return ms(values["p(50)"]), ms(values["p(99)"])


def requests(step: dict[str, Any]) -> int:
    return int(step["k6"]["http_reqs"]["values"]["count"])


def errors(step: dict[str, Any]) -> str:
    return percent(step["k6"]["http_req_failed"]["values"]["rate"])


def dropped(step: dict[str, Any]) -> int:
    return int(step["k6"].get("dropped_iterations", {}).get("values", {}).get("count", 0))


def crossing(rows: list[dict[str, Any]], unit: str) -> str:
    """The sentence under a rate table (D40o)."""
    if not rows:
        return "No steps were run."
    last = rows[-1]
    held = [r for r in rows if r.get("stopped") is None]
    if last.get("stopped") == "p99 passed 50 ms":
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
        lines.append(
            f"| {r['rate']} | {requests(r)} | {p50} | {p99} | {errors(r)} | {dropped(r)} |"
        )
    return [*lines, "", crossing(rows, "requests a second")]


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
            f"| {r['rate']} | {requests(r)} | {p50} | {p99} | {errors(r)} | {percent(timeouts)} "
            f"| {lag50} | {lag99} | {r['consumer_offset_lag_at_end']} |"
        )
    return [*lines, "", crossing(rows, "transfers a second")]


def hot_table(rows: list[dict[str, Any]]) -> list[str]:
    lines = [
        "| Preloaded events | Members read, median | p50 (ms) | p99 (ms) | Errors |",
        "|---:|---:|---:|---:|---:|",
    ]
    for r in rows:
        p50, p99 = latency(r)
        lines.append(
            f"| {r['preloaded']} | {r['members_read']['p50']} | {p50} | {p99} | {errors(r)} |"
        )
    over = [r for r in rows if r.get("stopped")]
    sentence = (
        f"p99 first passes 50 ms with **{over[0]['preloaded']} preloaded events** in the "
        "receiver's 24 h window."
        if over
        else "p99 stayed within 50 ms at every window size run."
    )
    return [*lines, "", sentence]


def conditions_line(c: dict[str, Any]) -> str:
    host, docker = c["host"], c["docker"]
    gib = 1024**3
    return (
        f"`{c['scenario']}`: {host['cpu']}, {host['cpus']} cores, "
        f"{host['memory_bytes'] / gib:.0f} GiB, {host['os']}; Docker {docker['version']}, its VM "
        f"{docker['vm_cpus']} CPUs and {docker['vm_memory_bytes'] / gib:.1f} GiB; commit "
        f"`{c['git_commit'][:7]}`, rule set v{c['rule_set_version']}; "
        f"{c['k6'].split(' (')[0]}; steps of {c['procedure']['step_seconds']} s."
    )


def render(folder: Path) -> tuple[str, list[str]]:
    """The README block for a results folder, and why it can't go in README.md (if it can't)."""
    problems = []
    conditions = {path.stem.split("-", 1)[1]: load(path) for path in folder.glob("conditions-*")}
    for scenario, c in sorted(conditions.items()):
        if not c["git_clean"]:
            problems.append(f"{scenario} ran from a tree with uncommitted changes")
        if c["procedure"]["step_seconds"] != PLAN_STEP_SECONDS:
            problems.append(f"{scenario}'s steps were {c['procedure']['step_seconds']} s")
    relative = folder.relative_to(ROOT) if folder.is_relative_to(ROOT) else folder
    out = [
        START,
        f"<!-- Made by `make report` from `{relative}/`; `make check-readme` fails if it "
        "differs. Don't edit by hand. -->",
        "",
    ]
    sections = (
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


def replace_block(readme: str, block: str) -> str:
    if readme.count(START) != 1 or readme.count(END) != 1:
        raise ReportError(f"README.md must hold one {START} and one {END}")
    before, rest = readme.split(START)
    _, after = rest.split(END)
    return before + block + after


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--results", type=Path, help="a results folder; default: the newest")
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--write", type=Path, metavar="README")
    action.add_argument("--check", type=Path, metavar="README")
    args = parser.parse_args(argv)
    try:
        folder = (args.results or latest(RESULTS)).resolve()
        block, problems = render(folder)
        if args.write is None and args.check is None:
            print(block)
            for problem in problems:
                print(f"not for README.md: {problem}", file=sys.stderr)
            return 0
        if problems:
            raise ReportError("these results can't go in README.md: " + "; ".join(problems))
        target: Path = args.write or args.check
        current = target.read_text()
        wanted = replace_block(current, block)
        if args.write is not None:
            target.write_text(wanted)
            return 0
        if current != wanted:
            print(
                f"{target}'s bench block differs from `make report`'s output for {folder.name}",
                file=sys.stderr,
            )
            return 1
        return 0
    except ReportError as exc:
        print(exc, file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
