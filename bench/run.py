"""`make bench`: the measurement plan (design.md section 12, D40o, D48.2, D57.2).

Four scenarios, run in this order in one session, each on its own throwaway Compose project,
`risk-engine-bench`, started empty and removed when it ends, so every run starts from the same
state and the normal stack's data is left alone (D49). The normal stack must be stopped first:
both use the same ports. Every scenario but direct-off runs with the shadow model on.

- direct-off: POST /v1/decisions at 50 requests a second, doubling every 3-minute step, until p99
  passes 50 ms, errors pass 1% or k6 can't hold the rate; with shadow scoring off, the baseline
  for the model's added latency (D57.2).
- direct: the same steps with shadow scoring on, as the service ships (D57.5).
- stub: the same steps against the stub's POST /transfers, so the decision call, the posting, the
  relay and the consumer run together.
- hot: every request to one receiver, from senders drawn at random, whose 24 h window is preloaded
  through Kafka to 1,000, 10,000, 100,000 and 1,000,000 events; 50 requests a second for 3 minutes
  at each size, until p99 passes 50 ms (D48.2, D50). A receiver, because each decision records an
  attempt on its sender: a hot sender's window would grow with the bench's own requests.

Each step writes k6's summary and a file of what the stack saw (decisions, how many held a shadow
score, feature lag, consumer offset lag, the stub's backup answers, the hot window's size); each
scenario writes a conditions file, which names the shadow model or null. Everything goes to
bench/results/<UTC date>/, and bench/report.py makes the README's tables from it.

    uv run --locked python -m bench.run [direct-off] [direct] [stub] [hot]

BENCH_STEP_SECONDS shortens every step for a quick try; the conditions file records it, and the
report refuses results whose steps aren't the plan's 3 minutes.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import platform
import subprocess
import sys
import time
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import psycopg
from confluent_kafka import Consumer, Producer, TopicPartition
from prometheus_client.parser import text_string_to_metric_families

from risk.features.consumer import GROUP_ID
from risk.rules import ruleset
from risk.settings import TOPIC
from risk.timestamps import format_instant

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "bench" / "results"
SCRIPT = ROOT / "bench" / "k6" / "load.js"
PROJECT = "risk-engine-bench"
NORMAL_PROJECT = "risk-engine"

API_URL = "http://127.0.0.1:8000"
STUB_URL = "http://127.0.0.1:8001"
KAFKA = "127.0.0.1:19092"
POSTGRES_PORT = 5433

# D40o: the plan's values.
STEP_S = 180
FIRST_RATE = 50
BUDGET_P99_MS = 50.0
ERROR_LIMIT = 0.01
# D49: the rest of the procedure.
DROP_LIMIT = 0.01  # dropped iterations over planned ones: k6 didn't hold the rate
MAX_RATE = 12_800  # a backstop; the stop rules end every run well before it on a laptop
ACCOUNTS = 10_000
HOT_SIZES = (1_000, 10_000, 100_000, 1_000_000)  # D48.2
HOT_RATE = 50
PRELOAD_WAIT_S = 1800
WINDOW_FEATURES = (
    "out_count_24h",
    "in_count_24h",
    "attempt_count_24h",
    "dest_in_count_24h",
    "dest_out_count_24h",
)
# In the order they run when none is named. direct-off is direct with shadow scoring off, the
# baseline for the model's added latency (D57.2); every other scenario runs with it on.
SCENARIOS = ("direct-off", "direct", "stub", "hot")
SHADOW_OFF = ("direct-off",)
MODEL_META = ROOT / "ml" / "model" / "model.json"


def env_file() -> Path:
    local = ROOT / ".env"
    return local if local.exists() else ROOT / ".env.example"


def read_env() -> dict[str, str]:
    values = {}
    for line in env_file().read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            values[key.strip()] = value.strip()
    return values


def run(
    args: Sequence[str],
    *,
    timeout: float = 600,
    check: bool = True,
    env: dict[str, str] | None = None,
) -> str:
    done = subprocess.run(
        list(args),
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
        env=env,
    )
    if check and done.returncode != 0:
        raise RuntimeError(f"{' '.join(args)} failed:\n{done.stdout}\n{done.stderr}")
    return done.stdout


def compose(
    project: str, *args: str, timeout: float = 600, env: dict[str, str] | None = None
) -> str:
    return run(
        ["docker", "compose", "--project-name", project, "--env-file", str(env_file()), *args],
        timeout=timeout,
        env=env,
    )


def shadow_env(shadow: bool) -> dict[str, str]:
    """The environment Compose interpolates: the API scores the committed model unless
    SHADOW_MODEL_DIR is empty (D57.5). A value from the caller's shell never leaks in."""
    env = {key: value for key, value in os.environ.items() if key != "SHADOW_MODEL_DIR"}
    if not shadow:
        env["SHADOW_MODEL_DIR"] = ""
    return env


def stack_up(shadow: bool) -> None:
    compose(PROJECT, "down", "--volumes", "--remove-orphans")
    compose(
        PROJECT,
        "up",
        "--detach",
        "--build",
        "--wait",
        "--wait-timeout",
        "300",
        timeout=900,
        env=shadow_env(shadow),
    )


def stack_down() -> None:
    compose(PROJECT, "down", "--volumes", "--remove-orphans")


def database() -> psycopg.Connection[tuple[Any, ...]]:
    values = read_env()
    return psycopg.connect(
        f"host=127.0.0.1 port={POSTGRES_PORT} user={values['POSTGRES_USER']} "
        f"password={values['POSTGRES_PASSWORD']} dbname={values['POSTGRES_DB']} connect_timeout=5",
        autocommit=True,
    )


def utc_now() -> datetime:
    return datetime.now(UTC)


# --- What the stack saw ------------------------------------------------------------------------


def counter_total(base_url: str, name: str) -> float:
    """The sum of a counter over all its labels, from a service's GET /metrics."""
    text = httpx.get(f"{base_url}/metrics", timeout=5).text
    total = 0.0
    for family in text_string_to_metric_families(text):
        for sample in family.samples:
            if sample.name == name:
                total += sample.value
    return total


def nearest_rank(values: list[int], q: float) -> int | None:
    """The q-quantile by the nearest-rank method: the smallest value with at least q of them at
    or below it."""
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(math.ceil(q * len(ordered)), 1) - 1]


@dataclass(frozen=True)
class Row:
    lag_ms: int | None  # None for a decision made before any event was applied (D43)
    features: dict[str, int]
    scored: bool  # the row holds a shadow score


def decisions_between(start: datetime, end: datetime) -> list[Row]:
    """Every decision requested in [start, end)."""
    with database() as conn:
        rows = conn.execute(
            "select feature_lag_ms, features, "
            "(extract(epoch from requested_at) * 1000)::bigint, shadow_score is not null "
            "from decisions where requested_at >= %s and requested_at < %s",
            (start, end),
        ).fetchall()
    return [
        Row(None if lag == t_ms else lag, features, scored) for lag, features, t_ms, scored in rows
    ]


def consumer_offset_lag() -> int:
    """Messages on ledger.entries the consumer group hasn't committed past."""
    consumer = Consumer({"bootstrap.servers": KAFKA, "group.id": GROUP_ID})
    try:
        partitions = [TopicPartition(TOPIC, p) for p in range(3)]
        committed = {tp.partition: tp.offset for tp in consumer.committed(partitions, timeout=10)}
        lag = 0
        for p in range(3):
            low, high = consumer.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=10)
            lag += high - (committed[p] if committed[p] >= 0 else low)
        return lag
    finally:
        consumer.close()


# --- Conditions ----------------------------------------------------------------------------------


def quiet(args: Sequence[str]) -> str:
    try:
        return run(args, timeout=60, check=False).strip()
    except (OSError, subprocess.TimeoutExpired):
        return ""


def tree_clean(root: Path = ROOT) -> bool:
    """Whether the tree matches its commit, leaving out bench/results/, where a run's earlier
    scenarios have already written their files. False when git itself fails."""
    try:
        done = subprocess.run(
            ["git", "status", "--porcelain", "--", ".", ":(exclude)bench/results"],
            cwd=root,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return done.returncode == 0 and done.stdout == ""


def conditions(scenario: str, step_s: int, shadow: bool) -> dict[str, Any]:
    rules = ruleset.load(ROOT / "risk" / "rules" / "ruleset.yaml")
    info = json.loads(quiet(["docker", "info", "--format", "{{json .}}"]) or "{}")
    images = json.loads(compose(PROJECT, "images", "--format", "json") or "[]")
    return {
        "scenario": scenario,
        "recorded_at": format_instant(utc_now()),
        "git_commit": quiet(["git", "rev-parse", "HEAD"]),
        "git_clean": tree_clean(),
        "rule_set_version": rules.version,
        "rule_set_sha256": rules.sha256,
        # The model the API was given; each step's file counts the decisions it scored.
        "shadow_model": json.loads(MODEL_META.read_text())["name"] if shadow else None,
        "host": {
            "cpu": quiet(["sysctl", "-n", "machdep.cpu.brand_string"]) or platform.processor(),
            "cpus": os.cpu_count(),
            "memory_bytes": int(quiet(["sysctl", "-n", "hw.memsize"]) or 0),
            "os": f"{platform.system()} {platform.release()} "
            f"{quiet(['sw_vers', '-productVersion'])}".strip(),
        },
        "docker": {
            "version": info.get("ServerVersion"),
            "vm_os": info.get("OperatingSystem"),
            "vm_cpus": info.get("NCPU"),
            "vm_memory_bytes": info.get("MemTotal"),
        },
        "images": sorted(
            {f"{image['Repository']}:{image['Tag']} {image['ID']}" for image in images}
        ),
        "k6": quiet(["k6", "version"]),
        "procedure": {
            "step_seconds": step_s,
            "first_rate": FIRST_RATE,
            "budget_p99_ms": BUDGET_P99_MS,
            "error_limit": ERROR_LIMIT,
            "drop_limit": DROP_LIMIT,
            "accounts": ACCOUNTS,
            "hot_sizes": list(HOT_SIZES),
            "hot_rate": HOT_RATE,
        },
    }


# --- One step ------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Outcome:
    p99_ms: float
    error_rate: float
    drop_rate: float

    def stop_reason(self) -> str | None:
        if self.p99_ms > BUDGET_P99_MS:
            return "p99 passed 50 ms"
        if self.error_rate > ERROR_LIMIT:
            return "errors passed 1%"
        if self.drop_rate > DROP_LIMIT:
            return "k6 couldn't hold the rate"
        return None


def k6_step(
    out: Path, name: str, target: str, base_url: str, rate: int, step_s: int, **extra: str
) -> tuple[dict[str, Any], datetime, datetime]:
    summary = out / f"{name}.json"
    env = {
        "TARGET": target,
        "BASE_URL": base_url,
        "RATE": str(rate),
        "DURATION": f"{step_s}s",
        "ACCOUNTS": str(ACCOUNTS),
        "RUN_ID": f"bench-{uuid.uuid4().hex[:12]}",
        "SUMMARY": str(summary),
        **extra,
    }
    args = ["k6", "run", "--quiet", "--no-usage-report"]
    for key, value in env.items():
        args += ["--env", f"{key}={value}"]
    started = utc_now()
    printed = run([*args, str(SCRIPT)], timeout=step_s + 600)
    ended = utc_now()
    print(printed.strip(), flush=True)
    return json.loads(summary.read_text()), started, ended


def outcome_of(summary: dict[str, Any], rate: int, step_s: int) -> Outcome:
    metrics = summary["metrics"]
    dropped = metrics.get("dropped_iterations", {}).get("values", {}).get("count", 0)
    return Outcome(
        p99_ms=metrics["http_req_duration"]["values"]["p(99)"],
        error_rate=metrics["http_req_failed"]["values"]["rate"],
        drop_rate=dropped / (rate * step_s),
    )


def write(path: Path, document: dict[str, Any]) -> None:
    path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")


def rate_steps(out: Path, scenario: str, step_s: int) -> None:
    target, base_url = ("transfers", STUB_URL) if scenario == "stub" else ("decisions", API_URL)
    rate = FIRST_RATE
    while rate <= MAX_RATE:
        name = f"{scenario}-{rate:05d}rps"
        transfers_before = counter_total(STUB_URL, "stub_transfers_total")
        backups_before = counter_total(STUB_URL, "stub_backup_answers_total")
        summary, started, ended = k6_step(out, name, target, base_url, rate, step_s)
        rows = decisions_between(started, ended + timedelta(seconds=1))
        lags = [row.lag_ms for row in rows if row.lag_ms is not None]
        transfers = counter_total(STUB_URL, "stub_transfers_total") - transfers_before
        backups = counter_total(STUB_URL, "stub_backup_answers_total") - backups_before
        result = outcome_of(summary, rate, step_s)
        stop = result.stop_reason()
        write(
            out / f"{name}-stack.json",
            {
                "scenario": scenario,
                "rate": rate,
                "summary": f"{name}.json",
                "started_at": format_instant(started),
                "ended_at": format_instant(ended),
                "decisions": len(rows),
                "shadow_scored": sum(row.scored for row in rows),
                "feature_lag_ms": {
                    "count": len(lags),
                    "p50": nearest_rank(lags, 0.5),
                    "p99": nearest_rank(lags, 0.99),
                    "max": max(lags) if lags else None,
                },
                "consumer_offset_lag_at_end": consumer_offset_lag(),
                "stub_transfers": int(transfers),
                "stub_backup_answers": int(backups),
                "stopped": stop,
            },
        )
        if stop is not None:
            print(f"{scenario}: stopped at {rate}/s: {stop}", flush=True)
            return
        rate *= 2


# --- The hot account -----------------------------------------------------------------------------


def account(n: int) -> str:
    """The bench's account n, from 1 to ACCOUNTS, spelt as bench/k6/load.js spells it."""
    return f"00000000-0000-4000-8000-{n:012x}"


def hot_event(hot: uuid.UUID, number: int, created_ms: int) -> tuple[str, dict[str, Any]]:
    """Preloaded transfer `number`: 100 cents into the hot account from one of the bench's
    accounts, in tally's event format (design.md 1.2). Returns its entry id and document."""
    created = format_instant(datetime.fromtimestamp(created_ms / 1000, UTC))
    entry_id = str(uuid.uuid4())
    return entry_id, {
        "id": entry_id,
        "kind": "transfer",
        "reversesId": None,
        "description": None,
        "effectiveAt": created,
        "createdAt": created,
        "decisionId": None,
        "riskCheck": "none",
        "lines": [
            {
                "id": 2 * number + 1,
                "accountId": account(1 + number % ACCOUNTS),
                "amount": -100,
                "currency": "CAD",
            },
            {"id": 2 * number + 2, "accountId": str(hot), "amount": 100, "currency": "CAD"},
        ],
    }


def preload(hot: uuid.UUID, start: int, total: int) -> None:
    """Publishes transfers into the hot account, numbered start to total - 1, with createdAt
    spread over the 23 hours before now; then waits until the consumer has committed past all of
    them."""
    producer = Producer(
        {
            "bootstrap.servers": KAFKA,
            "acks": "all",
            "linger.ms": 20,
            "queue.buffering.max.messages": 1_000_000,
        }
    )
    now_ms = int(time.time() * 1000)
    span_ms = 23 * 3600 * 1000
    count = total - start
    for n in range(count):
        created_ms = now_ms - span_ms + (n * span_ms) // max(count, 1)
        entry_id, document = hot_event(hot, start + n, created_ms)
        while True:
            try:
                producer.produce(TOPIC, key=entry_id.encode(), value=json.dumps(document).encode())
                break
            except BufferError:
                producer.poll(0.5)
        if n % 10_000 == 0:
            producer.poll(0)
    if producer.flush(120) != 0:
        raise RuntimeError("the broker didn't take every preloaded event within 120 s")
    deadline = time.monotonic() + PRELOAD_WAIT_S
    while consumer_offset_lag() > 0:
        if time.monotonic() > deadline:
            raise RuntimeError(f"the consumer didn't apply the preload within {PRELOAD_WAIT_S} s")
        time.sleep(2)


def hot_steps(out: Path, step_s: int) -> None:
    hot = uuid.uuid4()
    loaded = 0
    for size in HOT_SIZES:
        print(f"hot: preloading to {size} events", flush=True)
        preload(hot, loaded, size)
        loaded = size
        name = f"hot-{size:07d}"
        summary, started, ended = k6_step(
            out, name, "hot", API_URL, HOT_RATE, step_s, HOT_RECEIVER=str(hot)
        )
        rows = decisions_between(started, ended + timedelta(seconds=1))
        windows = [sum(row.features[f] for f in WINDOW_FEATURES) for row in rows]
        result = outcome_of(summary, HOT_RATE, step_s)
        stop = "p99 passed 50 ms" if result.p99_ms > BUDGET_P99_MS else None
        write(
            out / f"{name}-stack.json",
            {
                "scenario": "hot",
                "rate": HOT_RATE,
                "preloaded": size,
                "summary": f"{name}.json",
                "started_at": format_instant(started),
                "ended_at": format_instant(ended),
                "decisions": len(rows),
                "shadow_scored": sum(row.scored for row in rows),
                "members_read": {
                    "p50": nearest_rank(windows, 0.5),
                    "min": min(windows) if windows else None,
                    "max": max(windows) if windows else None,
                },
                "stopped": stop,
            },
        )
        if stop is not None:
            print(f"hot: stopped at {size} events: {stop}", flush=True)
            return


# --- Main ----------------------------------------------------------------------------------------


def normal_stack_running() -> bool:
    return bool(compose(NORMAL_PROJECT, "ps", "--quiet").strip())


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("scenarios", nargs="*", metavar="SCENARIO", help=" ".join(SCENARIOS))
    args = parser.parse_args(argv)
    scenarios = args.scenarios or list(SCENARIOS)
    unknown = sorted(set(scenarios) - set(SCENARIOS))
    if unknown:
        parser.error(f"unknown scenarios {unknown}; choose from {list(SCENARIOS)}")
    step_s = int(os.environ.get("BENCH_STEP_SECONDS") or STEP_S)
    if normal_stack_running():
        print("The normal stack is running and uses the same ports: run `make down` first.")
        return 2
    out = RESULTS / utc_now().strftime("%Y-%m-%d")
    out.mkdir(parents=True, exist_ok=True)
    for scenario in scenarios:
        print(f"{scenario}: starting a fresh stack", flush=True)
        shadow = scenario not in SHADOW_OFF
        try:
            stack_up(shadow)
            write(out / f"conditions-{scenario}.json", conditions(scenario, step_s, shadow))
            if scenario == "hot":
                hot_steps(out, step_s)
            else:
                rate_steps(out, scenario, step_s)
        finally:
            stack_down()
    print(f"results in {out.relative_to(ROOT)}; now run `make report`", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
