"""The migration runner's naming rules (D40w), without a database."""

from __future__ import annotations

from pathlib import Path

import pytest

from risk.migrate import MIGRATIONS_DIR, MigrationError, discover


def touch(directory: Path, *names: str) -> None:
    for name in names:
        (directory / name).write_text("select 1;")


def test_migrations_are_ordered_by_version(tmp_path: Path) -> None:
    touch(tmp_path, "0010_later.sql", "0002_second.sql", "0001_first.sql", "README.md")
    assert [m.version for m in discover(tmp_path)] == [1, 2, 10]


@pytest.mark.parametrize(
    "name", ["1_short.sql", "0001-dash.sql", "0001_Upper.sql", "0001.sql", "abcd_x.sql"]
)
def test_a_badly_named_migration_is_refused(tmp_path: Path, name: str) -> None:
    touch(tmp_path, name)
    with pytest.raises(MigrationError, match=r"0001_short_name\.sql"):
        discover(tmp_path)


def test_a_repeated_version_is_refused(tmp_path: Path) -> None:
    touch(tmp_path, "0001_one.sql", "0001_other.sql")
    with pytest.raises(MigrationError, match="also used by"):
        discover(tmp_path)


def test_version_zero_is_refused(tmp_path: Path) -> None:
    touch(tmp_path, "0000_zero.sql")
    with pytest.raises(MigrationError, match="start at 0001"):
        discover(tmp_path)


def test_a_missing_folder_is_refused(tmp_path: Path) -> None:
    with pytest.raises(MigrationError, match="no such migrations folder"):
        discover(tmp_path / "nope")


def test_the_repo_migrations_follow_the_rules() -> None:
    discover(MIGRATIONS_DIR)
