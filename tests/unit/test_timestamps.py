"""Timestamps as the contracts spell them: tally's writing, and the event parser's reading."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from risk.timestamps import epoch_ms, format_instant, parse_event_timestamp, truncate_to_ms


@pytest.mark.parametrize(
    ("micros", "text"),
    [
        (0, "2026-11-20T15:04:05Z"),
        (123000, "2026-11-20T15:04:05.123Z"),
        (100000, "2026-11-20T15:04:05.100Z"),
        (123456, "2026-11-20T15:04:05.123456Z"),
        (120, "2026-11-20T15:04:05.000120Z"),
    ],
)
def test_format_instant_writes_0_3_or_6_digits(micros: int, text: str) -> None:
    """Java's ISO_INSTANT on a microsecond value: whole groups of three digits, trailing zero
    groups dropped."""
    assert format_instant(datetime(2026, 11, 20, 15, 4, 5, micros, tzinfo=UTC)) == text


@pytest.mark.parametrize(
    "text",
    [
        "2026-11-20T15:04:05Z",
        "2026-11-20T15:04:05.123Z",
        "2026-11-20T15:04:05.123456Z",
        "2026-11-20T15:04:05.123456789Z",
    ],
)
def test_events_read_0_3_6_or_9_digits_with_a_z(text: str) -> None:
    parsed = truncate_to_ms(parse_event_timestamp(text))
    assert parsed.tzinfo is UTC
    assert parsed.microsecond in {0, 123000}


@pytest.mark.parametrize(
    "text",
    ["2026-11-20T15:04:05.12Z", "2026-11-20T15:04:05.1234Z", "2026-11-20T15:04:05+00:00"],
)
def test_events_refuse_other_shapes(text: str) -> None:
    with pytest.raises(ValueError, match="must"):
        parse_event_timestamp(text)


def test_epoch_ms_and_truncation() -> None:
    moment = datetime(1970, 1, 1, 0, 0, 1, 999999, tzinfo=UTC)
    assert epoch_ms(moment) == 1999
    assert truncate_to_ms(moment).microsecond == 999000
    assert epoch_ms(datetime(1969, 12, 31, 23, 59, 59, 999500, tzinfo=UTC)) == -1
