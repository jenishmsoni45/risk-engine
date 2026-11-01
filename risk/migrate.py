"""The migration runner (D40w): numbered SQL files applied in order, each once.

Run as `python -m risk.migrate` (the one-shot `migrate` Compose service). A migration is a file
named like `0001_short_name.sql` in risk/migrations/. Each one runs in its own transaction together
with its row in schema_migrations, so it is applied completely or not at all. An advisory lock lets
only one runner work at a time, so two runs at once still apply each migration once. A merged
migration is never edited; a change is a new file.
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from dataclasses import dataclass
from pathlib import Path

import psycopg

from risk import settings

log = logging.getLogger(__name__)

MIGRATIONS_DIR = Path(__file__).parent / "migrations"
FILE_NAME = re.compile(r"(?P<version>\d{4})_[a-z0-9_]+\.sql")
# Any fixed number works; it only has to be the same for every runner.
ADVISORY_LOCK_ID = 7_310_042

CREATE_TABLE = """
create table if not exists schema_migrations (
    version int primary key,
    applied_at timestamptz not null default now()
)
"""


class MigrationError(Exception):
    """The migrations folder breaks the naming rules."""


@dataclass(frozen=True)
class Migration:
    version: int
    path: Path


def discover(directory: Path) -> list[Migration]:
    """The .sql files in directory, by version. Other files (a README) are ignored."""
    found: dict[int, Migration] = {}
    for path in sorted(directory.glob("*.sql")):
        match = FILE_NAME.fullmatch(path.name)
        if match is None:
            raise MigrationError(f"{path.name}: a migration is named like 0001_short_name.sql")
        version = int(match["version"])
        if version == 0:
            raise MigrationError(f"{path.name}: versions start at 0001")
        if version in found:
            raise MigrationError(
                f"{path.name}: version {version} is also used by {found[version].path.name}"
            )
        found[version] = Migration(version, path)
    return [found[v] for v in sorted(found)]


def apply(conninfo: str, directory: Path = MIGRATIONS_DIR) -> list[int]:
    """Apply every migration not yet recorded; return the versions applied by this call."""
    migrations = discover(directory)
    applied_now: list[int] = []
    with psycopg.connect(conninfo, autocommit=True) as conn:
        # A session lock: closing the connection releases it, whatever happened.
        conn.execute("select pg_advisory_lock(%s)", (ADVISORY_LOCK_ID,))
        conn.execute(CREATE_TABLE)
        rows = conn.execute("select version from schema_migrations").fetchall()
        done = {version for (version,) in rows}
        for migration in migrations:
            if migration.version in done:
                continue
            with conn.transaction():
                # bytes, not str: the file is trusted SQL from the repo, run as written.
                conn.execute(migration.path.read_bytes())
                conn.execute(
                    "insert into schema_migrations (version) values (%s)", (migration.version,)
                )
            log.info("applied migration %s", migration.path.name)
            applied_now.append(migration.version)
    return applied_now


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description="Apply the SQL migrations in order.")
    parser.add_argument("--dir", type=Path, default=MIGRATIONS_DIR, help="migrations folder")
    args = parser.parse_args(argv)
    try:
        applied = apply(settings.database_url(), args.dir)
    except (MigrationError, psycopg.Error, RuntimeError) as exc:
        log.error("migrate failed: %s", exc)
        return 1
    log.info("migrate done: %d applied", len(applied))
    return 0


if __name__ == "__main__":
    sys.exit(main())
