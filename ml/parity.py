"""I7's check (design.md section 6): the features the decision API logged for each replayed
transfer equal the offline pipeline's recompute over the same transfers, exactly (D36).

`compare` does the comparison and counts, per feature family and window, how many transfers had
members at all, so a report shows that the equality isn't between empty windows. `make parity`
runs `main`: the first 10,000 transfers of the owner's PaySim copy (D52c), replayed on a
throwaway stack (D52a), then compared. It needs the normal stack stopped, since both use the same
ports, and removes its stack and volumes at the end.
"""

from __future__ import annotations

import argparse
import itertools
import subprocess
import sys
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import psycopg

from ml.offline import OfflineRow, features
from ml.paysim import Transfer, load
from ml.replay import (
    REDIS_URL,
    SAMPLE_SIZE,
    STUB_URL,
    RedisWatermark,
    ReplayError,
    print_progress,
    replay,
)
from risk.features.compute import FAMILIES, FEATURE_NAMES, WINDOWS

ROOT = Path(__file__).resolve().parent.parent
PROJECT = "risk-engine-parity"
NORMAL_PROJECT = "risk-engine"
COMPOSE_FILES = ("docker-compose.yml", "docker-compose.parity.yml")
POSTGRES_PORT = 5433
SHOWN_MISMATCHES = 10


@dataclass(frozen=True)
class Mismatch:
    request_id: str
    line: int
    differences: dict[str, tuple[int | None, int]]  # name: (logged online, offline)


@dataclass
class ParityReport:
    compared: int = 0
    equal: int = 0
    missing: list[str] = field(default_factory=list)  # replayed, but no decision row
    mismatches: list[Mismatch] = field(default_factory=list)
    # "{family}_count_{window}": transfers whose window held at least one member.
    nonzero: dict[str, int] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.compared > 0 and self.equal == self.compared

    def lines(self) -> list[str]:
        out = [
            f"compared {self.compared} transfers: {self.equal} equal, "
            f"{len(self.mismatches)} different, {len(self.missing)} without a decision row",
            "transfers whose window held at least one member, in the offline recompute:",
        ]
        for family in FAMILIES:
            counts = ", ".join(
                f"{window} {self.nonzero.get(f'{family}_count_{window}', 0)}"
                for window, _ in WINDOWS
            )
            out.append(f"  {family}: {counts}")
        for request_id in self.missing[:SHOWN_MISMATCHES]:
            out.append(f"no decision row for requestId {request_id}")
        for mismatch in self.mismatches[:SHOWN_MISMATCHES]:
            out.append(
                f"line {mismatch.line}, requestId {mismatch.request_id}: "
                + ", ".join(
                    f"{name} online {online} offline {offline}"
                    for name, (online, offline) in mismatch.differences.items()
                )
            )
        out.append(
            "I7 holds: every logged snapshot equals the offline recompute"
            if self.ok
            else "I7 FAILS"
        )
        return out


def compare(rows: Iterable[OfflineRow], logged: Mapping[str, Mapping[str, Any]]) -> ParityReport:
    """Each offline row against the snapshot logged for its requestId. JSON numbers come back
    from Postgres as Python ints, so equality is exact."""
    report = ParityReport()
    for row in rows:
        transfer = row.transfer
        report.compared += 1
        for family in FAMILIES:
            for window, _ in WINDOWS:
                name = f"{family}_count_{window}"
                if row.features[name]:
                    report.nonzero[name] = report.nonzero.get(name, 0) + 1
        online = logged.get(transfer.request_id)
        if online is None:
            report.missing.append(transfer.request_id)
            continue
        differences = {
            name: (online.get(name), row.features[name])
            for name in FEATURE_NAMES
            if online.get(name) != row.features[name]
        }
        extra = sorted(set(online) - set(FEATURE_NAMES))
        differences.update({name: (online[name], 0) for name in extra})
        if differences:
            report.mismatches.append(Mismatch(transfer.request_id, transfer.line, differences))
        else:
            report.equal += 1
    return report


def logged_features(
    conn: psycopg.Connection[tuple[Any, ...]], request_ids: Sequence[str]
) -> dict[str, dict[str, Any]]:
    """The decision log's feature snapshot for each requestId that has a row."""
    rows = conn.execute(
        "select request_id, features from decisions where request_id = any(%s)",
        (list(request_ids),),
    ).fetchall()
    return dict(rows)


def check(transfers: Sequence[Transfer], conn: psycopg.Connection[tuple[Any, ...]]) -> ParityReport:
    """I7 for transfers already replayed: their logged snapshots against the offline recompute."""
    logged = logged_features(conn, [t.request_id for t in transfers])
    return compare(features(transfers), logged)


# --- make parity -----------------------------------------------------------------------------


def env_file() -> Path:
    """The file Compose reads, chosen as the Makefile does: .env, else .env.example."""
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


def compose(project: str, *args: str, timeout: float = 900) -> str:
    files = [arg for name in COMPOSE_FILES for arg in ("--file", name)]
    command = ["docker", "compose", "--project-name", project, "--env-file", str(env_file())]
    command += [*files, *args]
    done = subprocess.run(
        command, cwd=ROOT, capture_output=True, text=True, timeout=timeout, check=False
    )
    if done.returncode != 0:
        raise RuntimeError(f"{' '.join(command)} failed:\n{done.stdout}\n{done.stderr}")
    return done.stdout


def database() -> psycopg.Connection[tuple[Any, ...]]:
    values = read_env()
    return psycopg.connect(
        f"host=127.0.0.1 port={POSTGRES_PORT} user={values['POSTGRES_USER']} "
        f"password={values['POSTGRES_PASSWORD']} dbname={values['POSTGRES_DB']} connect_timeout=5",
        autocommit=True,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--csv", type=Path, required=True, help="a PaySim CSV")
    parser.add_argument("--count", type=int, default=SAMPLE_SIZE, help="the first COUNT transfers")
    args = parser.parse_args(argv)
    if not args.csv.is_file():
        print(f"{args.csv} doesn't exist: put the PaySim CSV in data/ (CLAUDE.md)", file=sys.stderr)
        return 2
    if compose(NORMAL_PROJECT, "ps", "--quiet").strip():
        print("The normal stack is running and uses the same ports: run `make down` first.")
        return 2
    transfers = list(itertools.islice(load(args.csv), args.count))
    print(f"loaded the first {len(transfers)} transfers of {args.csv}", flush=True)
    try:
        print(f"starting a fresh stack, {PROJECT}, with the stub in replay mode", flush=True)
        compose(PROJECT, "down", "--volumes", "--remove-orphans")
        compose(PROJECT, "up", "--detach", "--build", "--wait", "--wait-timeout", "300")
        watermark = RedisWatermark(REDIS_URL)
        try:
            result = replay(transfers, STUB_URL, watermark, progress=print_progress(len(transfers)))
        except ReplayError as exc:
            print(f"replay stopped: {exc}", file=sys.stderr)
            return 1
        finally:
            watermark.close()
        print(f"replayed {result.transfers} transfers in {result.elapsed_s:.1f} s", flush=True)
        with database() as conn:
            report = check(transfers, conn)
    finally:
        print(f"removing {PROJECT} and its volumes", flush=True)
        compose(PROJECT, "down", "--volumes", "--remove-orphans")
    for line in report.lines():
        print(line)
    return 0 if report.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
