"""The migration runner against the stack's Postgres (D40w): each migration applies once.

Phase 1 ships no migration of its own (the first is Phase 2's `decisions` table), so these tests
give the runner throwaway migrations in a database created for each test. The last test runs the
real `migrate` service twice against the real database.
"""

from __future__ import annotations

import threading
import uuid
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest
from psycopg import sql

from risk.migrate import MIGRATIONS_DIR, apply, discover
from tests.helpers import compose, postgres_conninfo


@pytest.fixture
def database() -> Iterator[str]:
    """A fresh, empty database for one test; dropped afterwards."""
    name = f"migrate_test_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(postgres_conninfo(), autocommit=True) as admin:
        admin.execute(sql.SQL("create database {}").format(sql.Identifier(name)))
    try:
        yield postgres_conninfo(dbname=name)
    finally:
        with psycopg.connect(postgres_conninfo(), autocommit=True) as admin:
            admin.execute(
                sql.SQL("drop database if exists {} with (force)").format(sql.Identifier(name))
            )


def write(directory: Path, name: str, text: str) -> None:
    (directory / name).write_text(text)


def recorded_versions(conninfo: str) -> list[int]:
    with psycopg.connect(conninfo) as conn:
        rows = conn.execute("select version from schema_migrations order by version").fetchall()
    return [version for (version,) in rows]


def row_count(conninfo: str) -> int:
    with psycopg.connect(conninfo) as conn:
        row = conn.execute("select count(*) from things").fetchone()
    assert row is not None
    count: int = row[0]
    return count


@pytest.fixture
def two_migrations(tmp_path: Path) -> Path:
    write(tmp_path, "0001_create_things.sql", "create table things (id int primary key);")
    # Not idempotent on purpose: applied twice, it would fail on the primary key.
    write(tmp_path, "0002_insert_thing.sql", "insert into things (id) values (1);")
    return tmp_path


def test_running_twice_applies_each_migration_once(database: str, two_migrations: Path) -> None:
    assert apply(database, two_migrations) == [1, 2]
    assert apply(database, two_migrations) == []
    assert recorded_versions(database) == [1, 2]
    assert row_count(database) == 1


def test_a_new_migration_is_applied_alone(database: str, two_migrations: Path) -> None:
    apply(database, two_migrations)
    write(two_migrations, "0003_insert_another.sql", "insert into things (id) values (2);")
    assert apply(database, two_migrations) == [3]
    assert recorded_versions(database) == [1, 2, 3]
    assert row_count(database) == 2


def test_concurrent_runs_apply_each_migration_once(database: str, two_migrations: Path) -> None:
    results: list[list[int]] = []
    errors: list[BaseException] = []
    lock = threading.Lock()

    def run() -> None:
        try:
            applied = apply(database, two_migrations)
        except BaseException as exc:  # noqa: BLE001 - reported by the assertion below
            with lock:
                errors.append(exc)
            return
        with lock:
            results.append(applied)

    threads = [threading.Thread(target=run) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
    assert errors == []
    assert sorted(v for applied in results for v in applied) == [1, 2]
    assert recorded_versions(database) == [1, 2]
    assert row_count(database) == 1


def test_a_failing_migration_leaves_nothing_behind(database: str, tmp_path: Path) -> None:
    write(tmp_path, "0001_create_things.sql", "create table things (id int primary key);")
    write(
        tmp_path,
        "0002_half_broken.sql",
        "insert into things (id) values (1);\nselect no_such_function();",
    )
    with pytest.raises(psycopg.Error):
        apply(database, tmp_path)
    assert recorded_versions(database) == [1]
    assert row_count(database) == 0


def test_compose_migrate_runs_twice() -> None:
    expected = [migration.version for migration in discover(MIGRATIONS_DIR)]
    for _ in range(2):
        result = compose("run", "--rm", "--no-deps", "migrate", check=False)
        assert result.returncode == 0, result.stderr
    assert recorded_versions(postgres_conninfo()) == expected
