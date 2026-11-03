"""The PaySim profile (design.md section 8, D32): what the owner's PaySim copy holds, written to
ml/reports/ before the sample is confirmed.

It reads the CSV twice. The first pass counts every row by type: rows, fraud, PaySim's own flag,
amounts that round to 0 cents, and loaded rows that send to the same account. The second loads the
sample (TRANSFER and CASH_OUT, D32) and reports:
- how often senders and receivers repeat (PLAN.md Q12), and how often each feature family has
  members at the transfer's time, from the offline pipeline (D19), overall and for fraud;
- the amounts, against the starter rules' thresholds;
- transfers and fraud per hour of day under D51.2's mapping, to check where the nightly lull falls;
- what the starter rule set would decide for every transfer, since its thresholds are CAD cents
  and PaySim's currency is unnamed (phases.md, Phase 6).

Every number in the report comes from this script (D29). Run `make profile`.
"""

from __future__ import annotations

import argparse
import hashlib
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path

from ml.offline import features
from ml.paysim import LOADED_TYPES, STEP_1, TYPES, LoadStats, PaySimError, cents, load, read_rows
from risk.features.compute import FAMILIES, WINDOWS
from risk.rules import ruleset

ROOT = Path(__file__).resolve().parent.parent
RULES = ROOT / "risk" / "rules" / "ruleset.yaml"
REPORT = ROOT / "ml" / "reports" / "paysim_profile.md"
REPEAT_BANDS = ((1, 1), (2, 2), (3, 9), (10, None))
PERCENTILES = (50, 90, 99)


@dataclass
class RawCounts:
    """The first pass: every row of the file."""

    rows: int = 0
    steps: tuple[int, int] | None = None
    by_type: Counter[str] = field(default_factory=Counter)
    fraud: Counter[str] = field(default_factory=Counter)
    flagged: Counter[str] = field(default_factory=Counter)
    zero_cents: Counter[str] = field(default_factory=Counter)
    to_self: int = 0  # loaded types with a non-zero amount, nameOrig equal to nameDest


@dataclass
class SampleCounts:
    """The second pass: the loaded transfers."""

    transfers: int = 0
    fraud: int = 0
    senders: Counter[int] = field(default_factory=Counter)
    receivers: Counter[int] = field(default_factory=Counter)
    fraud_accounts: list[tuple[int, int]] = field(default_factory=list)
    amounts: list[int] = field(default_factory=list)
    by_hour: Counter[int] = field(default_factory=Counter)
    fraud_by_hour: Counter[int] = field(default_factory=Counter)
    # (feature name, is fraud): transfers whose window held at least one member.
    nonzero: Counter[tuple[str, bool]] = field(default_factory=Counter)
    decisions: Counter[tuple[str, bool]] = field(default_factory=Counter)
    fired: Counter[tuple[str, bool]] = field(default_factory=Counter)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()


def raw_counts(path: Path) -> RawCounts:
    counts = RawCounts()
    low = high = None
    for row in read_rows(path):
        counts.rows += 1
        low = row.step if low is None else min(low, row.step)
        high = row.step if high is None else max(high, row.step)
        counts.by_type[row.type] += 1
        counts.fraud[row.type] += row.is_fraud
        counts.flagged[row.type] += row.is_flagged_fraud
        zero = cents(row.amount) == 0
        counts.zero_cents[row.type] += zero
        if row.type in LOADED_TYPES and not zero and row.name_orig == row.name_dest:
            counts.to_self += 1
    if low is not None and high is not None:
        counts.steps = (low, high)
    return counts


def sample_counts(path: Path, rules: ruleset.RuleSet, stats: LoadStats) -> SampleCounts:
    counts = SampleCounts()
    for row in features(load(path, stats=stats)):
        transfer, fraud = row.transfer, row.transfer.is_fraud
        counts.transfers += 1
        counts.fraud += fraud
        counts.senders[transfer.sender.int] += 1
        counts.receivers[transfer.receiver.int] += 1
        if fraud:
            counts.fraud_accounts.append((transfer.sender.int, transfer.receiver.int))
        counts.amounts.append(transfer.amount)
        counts.by_hour[row.hour] += 1
        counts.fraud_by_hour[row.hour] += fraud
        for family in FAMILIES:
            for window, _ in WINDOWS:
                name = f"{family}_count_{window}"
                if row.features[name]:
                    counts.nonzero[name, fraud] += 1
        decision, fired = rules.evaluate(row.features)
        counts.decisions[decision, fraud] += 1
        for rule_id in fired:
            counts.fired[rule_id, fraud] += 1
    return counts


# --- The report ------------------------------------------------------------------------------


def share(part: int, whole: int) -> str:
    return f"{100 * part / whole:.2f}%" if whole else "n/a"


def table(header: Sequence[str], rows: Iterable[Sequence[object]]) -> list[str]:
    lines = [
        "| " + " | ".join(header) + " |",
        "|" + "|".join("---" for _ in header) + "|",
    ]
    lines += ["| " + " | ".join(str(cell) for cell in row) + " |" for row in rows]
    return lines


def band(low: int, high: int | None) -> str:
    if high is None:
        return f"{low}+"
    return str(low) if low == high else f"{low} to {high}"


def repetition(counts: Counter[int]) -> list[object]:
    """Accounts, then accounts and their transfers in each band of transfers per account."""
    cells: list[object] = [len(counts)]
    for low, high in REPEAT_BANDS:
        inside = [n for n in counts.values() if n >= low and (high is None or n <= high)]
        cells.append(f"{len(inside)} accounts, {sum(inside)} transfers")
    return cells


def percentile(ordered: Sequence[int], q: int) -> int:
    """Nearest rank on sorted values."""
    rank = max(1, -(-q * len(ordered) // 100))
    return ordered[rank - 1]


def report(
    path: Path, digest: str, raw: RawCounts, stats: LoadStats | None,
    sample: SampleCounts | None, rules: ruleset.RuleSet, error: str | None,
) -> str:  # fmt: skip
    lines = [
        "# PaySim profile",
        "",
        "Written by `make profile` (`python -m ml.profile`); every number below comes from that "
        "script (D29). It confirms the sample before Phase 7 (D32).",
        "",
        f"- File: `{path.name}`, {path.stat().st_size} bytes, SHA-256 `{digest}`",
        f"- Rows: {raw.rows}; steps {raw.steps[0]} to {raw.steps[1]}"
        if raw.steps
        else f"- Rows: {raw.rows}",
        f"- Step 1 starts at {STEP_1.isoformat().replace('+00:00', 'Z')} (D51.2)",
        "",
        "## Rows by type",
        "",
        *table(
            ("type", "rows", "isFraud", "isFlaggedFraud", "amount rounds to 0 cents"),
            ((t, raw.by_type[t], raw.fraud[t], raw.flagged[t], raw.zero_cents[t]) for t in TYPES),
        ),
        "",
        f"Rows of the loaded types (D32: {', '.join(LOADED_TYPES)}) that send to the same "
        f"account, which the loader refuses (D52e): {raw.to_self}.",
    ]
    if error is not None or stats is None or sample is None:
        lines += ["", f"**The loader refuses this file:** {error}", ""]
        return "\n".join(lines)

    fraud, legit = sample.fraud, sample.transfers - sample.fraud
    amounts = sorted(sample.amounts)
    fraud_senders_repeat = sum(1 for s, _ in sample.fraud_accounts if sample.senders[s] > 1)
    fraud_receivers_repeat = sum(1 for _, r in sample.fraud_accounts if sample.receivers[r] > 1)
    lines += [
        "",
        "## The sample",
        "",
        f"- Transfers loaded: {sample.transfers}, of which fraud {fraud} "
        f"({share(fraud, sample.transfers)})",
        f"- Skipped because the amount rounds to 0 cents (D52f): {stats.zero_amount}",
        "",
        "## How often accounts repeat",
        "",
        "Transfers per account over the whole sample.",
        "",
        *table(
            ("side", "accounts", *(f"with {band(lo, hi)}" for lo, hi in REPEAT_BANDS)),
            (
                ("senders", *repetition(sample.senders)),
                ("receivers", *repetition(sample.receivers)),
            ),
        ),
        "",
        f"- Fraud transfers whose sender sends more than once: {fraud_senders_repeat} "
        f"({share(fraud_senders_repeat, fraud)})",
        f"- Fraud transfers whose receiver receives more than once: {fraud_receivers_repeat} "
        f"({share(fraud_receivers_repeat, fraud)})",
        "",
        "## Which feature families have members",
        "",
        "Share of transfers whose window held at least one member at the transfer's time, from "
        "the offline pipeline (D19).",
        "",
        *table(
            ("family", *(f"{w}, all" for w, _ in WINDOWS), "24h, fraud", "24h, not fraud"),
            (
                (
                    family,
                    *(
                        share(
                            sample.nonzero[f"{family}_count_{w}", True]
                            + sample.nonzero[f"{family}_count_{w}", False],
                            sample.transfers,
                        )
                        for w, _ in WINDOWS
                    ),
                    share(sample.nonzero[f"{family}_count_24h", True], fraud),
                    share(sample.nonzero[f"{family}_count_24h", False], legit),
                )
                for family in FAMILIES
            ),
        ),
        "",
        "## Amounts, in cents",
        "",
        *table(
            ("", *(f"p{q}" for q in PERCENTILES), "max", "≥ 200,000", "≥ 1,000,000"),
            (
                (
                    "all transfers",
                    *(percentile(amounts, q) for q in PERCENTILES),
                    amounts[-1],
                    share(sum(a >= 200_000 for a in amounts), len(amounts)),
                    share(sum(a >= 1_000_000 for a in amounts), len(amounts)),
                ),
            ),
        ),
        "",
        "200,000 and 1,000,000 are the starter rules' amount thresholds (D20).",
        "",
        "## Hour of day (D51.2)",
        "",
        *table(
            ("hour (UTC)", "transfers", "fraud"),
            ((h, sample.by_hour[h], sample.fraud_by_hour[h]) for h in range(24)),
        ),
        "",
        f"## The starter rules on PaySim (rule set {rules.name})",
        "",
        "What the starter rule set (D20) decides for every loaded transfer, from its offline "
        "features. Its thresholds assume CAD cents; PaySim's currency is unnamed. Over every "
        "day of the sample, not only Phase 7's test days.",
        "",
        *table(
            ("decision", "transfers", "share", "of fraud", "of not fraud"),
            (
                (
                    decision,
                    sample.decisions[decision, True] + sample.decisions[decision, False],
                    share(
                        sample.decisions[decision, True] + sample.decisions[decision, False],
                        sample.transfers,
                    ),
                    share(sample.decisions[decision, True], fraud),
                    share(sample.decisions[decision, False], legit),
                )
                for decision in ("allow", "hold", "block")
            ),
        ),
        "",
        *table(
            ("rule", "fired on", "share", "of fraud", "of not fraud"),
            (
                (
                    rule.id,
                    sample.fired[rule.id, True] + sample.fired[rule.id, False],
                    share(
                        sample.fired[rule.id, True] + sample.fired[rule.id, False],
                        sample.transfers,
                    ),
                    share(sample.fired[rule.id, True], fraud),
                    share(sample.fired[rule.id, False], legit),
                )
                for rule in rules.rules
            ),
        ),
        "",
    ]
    return "\n".join(lines)


def profile(path: Path, out: Path, rules_path: Path = RULES) -> bool:
    """Writes the report; False when the loader refuses the file (the report says why)."""
    rules = ruleset.load(rules_path)
    digest = sha256(path)
    raw = raw_counts(path)
    stats = LoadStats()
    sample: SampleCounts | None = None
    error: str | None = None
    try:
        sample = sample_counts(path, rules, stats)
    except PaySimError as exc:
        error = str(exc)
    if sample is not None and sample.transfers == 0:
        error = f"no {' or '.join(LOADED_TYPES)} row with an amount of at least 1 cent"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(report(path, digest, raw, stats, sample, rules, error))
    return error is None


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--csv", type=Path, required=True, help="a PaySim CSV")
    parser.add_argument("--out", type=Path, default=REPORT)
    args = parser.parse_args(argv)
    if not args.csv.is_file():
        print(f"{args.csv} doesn't exist: put the PaySim CSV in data/ (CLAUDE.md)")
        return 2
    ok = profile(args.csv, args.out)
    print(f"wrote {args.out}" + ("" if ok else "; the loader refuses this file, see the report"))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
