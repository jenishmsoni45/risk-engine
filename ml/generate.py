"""A seeded generator of PaySim-format rows (D37), so CI can replay, profile and check parity
without the real CSV, which is never committed (CLAUDE.md).

The rows have PaySim's columns and types, in step order, with accounts that repeat so every
feature family has members, and with PaySim's fraud pattern planted: a TRANSFER from a victim to a
mule, then the mule's CASH_OUT of the same amount, both marked isFraud. One busy step packs
transfers tightly enough for one-minute windows, and holds a burst from one sender and many
senders paying one mule. Generated data says nothing about real PaySim; it only exercises the code.

Run `python -m ml.generate --out data/generated.csv` to try the pipeline on it.
"""

from __future__ import annotations

import argparse
import csv
import random
from collections.abc import Sequence
from pathlib import Path

from ml.paysim import COLUMNS

BUSY_STEP = 10
BUSY_TRANSFERS = 400  # 9 s apart, so one sender's burst fills a one-minute window
BURST = 7  # transfers in a row from one sender, inside a minute
FAN_IN = 22  # senders paying one mule within the busy hour
QUIET_ROWS = (0, 24)  # rows of every type in each other step
NORMAL_TYPES = ("PAYMENT", "CASH_OUT", "TRANSFER", "CASH_IN", "DEBIT")
NORMAL_WEIGHTS = (30, 30, 15, 20, 5)

Row = tuple[str, ...]


def _name(rng: random.Random, prefix: str) -> str:
    return f"{prefix}{rng.randrange(10**8, 10**10)}"


def _money(value: float) -> str:
    return f"{value:.2f}"


class _Generator:
    def __init__(self, seed: int) -> None:
        self.rng = random.Random(seed)
        self.customers = [_name(self.rng, "C") for _ in range(80)]
        self.merchants = [_name(self.rng, "M") for _ in range(20)]
        self.mules = self.customers[:4]

    def row(
        self, step: int, type_: str, orig: str, dest: str, amount: float, fraud: bool = False
    ) -> Row:
        old_orig = round(amount + self.rng.uniform(0, 20_000), 2)
        new_orig = round(max(old_orig - amount, 0.0), 2)
        old_dest = round(self.rng.uniform(0, 50_000), 2)
        new_dest = round(old_dest + amount, 2)
        # PaySim flags a TRANSFER over 200,000 as suspicious.
        flagged = type_ == "TRANSFER" and amount > 200_000
        return (
            str(step),
            type_,
            _money(amount),
            orig,
            _money(old_orig),
            _money(new_orig),
            dest,
            _money(old_dest),
            _money(new_dest),
            "1" if fraud else "0",
            "1" if flagged else "0",
        )

    def other(self, orig: str) -> str:
        while (dest := self.rng.choice(self.customers)) == orig:
            pass
        return dest

    def normal(self, step: int, types: Sequence[str] = NORMAL_TYPES) -> Row:
        weights = [NORMAL_WEIGHTS[NORMAL_TYPES.index(t)] for t in types]
        type_ = self.rng.choices(types, weights)[0]
        orig = self.rng.choice(self.customers)
        dest = self.rng.choice(self.merchants) if type_ == "PAYMENT" else self.other(orig)
        return self.row(step, type_, orig, dest, round(self.rng.uniform(1, 5_000), 2))

    def fraud_pair(self, step: int) -> list[Row]:
        """PaySim's pattern: the victim's balance to a mule, then the mule cashes it out."""
        victim = self.rng.choice(self.customers[len(self.mules) :])
        mule = self.rng.choice(self.mules)
        amount = round(self.rng.uniform(2_000, 60_000), 2)
        return [
            self.row(step, "TRANSFER", victim, mule, amount, fraud=True),
            self.row(step, "CASH_OUT", mule, self.other(mule), amount, fraud=True),
        ]

    def busy(self, step: int) -> list[Row]:
        rows = [self.normal(step, ("TRANSFER", "CASH_OUT")) for _ in range(BUSY_TRANSFERS)]
        rows += [self.normal(step, ("PAYMENT", "CASH_IN", "DEBIT")) for _ in range(40)]
        self.rng.shuffle(rows)
        sender = self.customers[-1]
        burst = [
            self.row(step, "TRANSFER", sender, self.other(sender), 50.0 + n) for n in range(BURST)
        ]
        mule = self.mules[0]
        payers = self.rng.sample(self.customers[len(self.mules) : -1], FAN_IN)
        fan_in = [self.row(step, "TRANSFER", p, mule, 120.0) for p in payers]
        at = self.rng.randrange(len(rows))
        rows[at:at] = burst
        for row in fan_in:
            rows.insert(self.rng.randrange(len(rows) + 1), row)
        return rows

    def quiet(self, step: int) -> list[Row]:
        rows = [self.normal(step) for _ in range(self.rng.randint(*QUIET_ROWS))]
        if step % 3 == 0:
            at = self.rng.randrange(len(rows) + 1)
            rows[at:at] = self.fraud_pair(step)
        if step % 5 == 0:
            # A large transfer, and a day's outflow from one sender building up across steps.
            orig = self.customers[-2]
            rows.insert(0, self.row(step, "TRANSFER", orig, self.other(orig), 10_000.0))
        if step % 11 == 0:
            # PaySim holds fraud CASH_OUTs of 0, which the loader skips (D52f).
            mule = self.rng.choice(self.mules)
            rows.append(self.row(step, "CASH_OUT", mule, self.other(mule), 0.0, fraud=True))
        return rows


def generate(seed: int = 1, steps: int = 30) -> list[Row]:
    """Rows for steps 1 to `steps`, in step order; the same seed always gives the same rows."""
    generator = _Generator(seed)
    rows: list[Row] = []
    for step in range(1, steps + 1):
        rows += generator.busy(step) if step == BUSY_STEP else generator.quiet(step)
    return rows


def write_csv(path: Path, rows: Sequence[Row]) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(COLUMNS)
        writer.writerows(rows)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--steps", type=int, default=30)
    args = parser.parse_args(argv)
    rows = generate(args.seed, args.steps)
    write_csv(args.out, rows)
    print(f"wrote {len(rows)} rows to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
