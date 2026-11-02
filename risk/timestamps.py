"""Timestamps as the contracts spell them (design.md 1.1, 1.2 and 1.3).

Parsing is strict about the shape and returns an aware UTC datetime. Python keeps microseconds, so
a 7 to 9 digit fraction is cut to 6 here; callers that need milliseconds truncate again.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta, timezone

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
ONE_MS = timedelta(milliseconds=1)

_TIMESTAMP = re.compile(
    r"(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})"
    r"T(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})"
    r"(?:\.(?P<fraction>\d{1,9}))?"
    r"(?P<offset>Z|[+-]\d{2}:\d{2})"
)


def _parse(text: object, *, fraction_digits: frozenset[int] | None, zulu_only: bool) -> datetime:
    if not isinstance(text, str):
        raise ValueError("must be a string")
    match = _TIMESTAMP.fullmatch(text)
    if match is None:
        raise ValueError("must be an ISO-8601 timestamp such as 2026-11-20T15:04:05.123Z")
    fraction = match["fraction"] or ""
    if fraction_digits is not None and len(fraction) not in fraction_digits:
        raise ValueError(f"must have {sorted(fraction_digits)} fraction digits")
    offset = match["offset"]
    if zulu_only and offset != "Z":
        raise ValueError("must be in UTC, ending in Z")
    if offset == "Z":
        tz = UTC
    else:
        hours, minutes = int(offset[1:3]), int(offset[4:6])
        if hours > 23 or minutes > 59:
            raise ValueError("has an impossible offset")
        delta = timedelta(hours=hours, minutes=minutes)
        tz = timezone(-delta if offset[0] == "-" else delta)
    try:
        parsed = datetime(
            int(match["year"]),
            int(match["month"]),
            int(match["day"]),
            int(match["hour"]),
            int(match["minute"]),
            int(match["second"]),
            int(fraction[:6].ljust(6, "0")),
            tzinfo=tz,
        )
        return parsed.astimezone(UTC)
    except (ValueError, OverflowError) as exc:
        raise ValueError("is not a valid date and time") from exc


def parse_offset_timestamp(text: object) -> datetime:
    """ISO-8601 with a Z or a ±HH:MM offset and 0 to 9 fraction digits, as UTC."""
    return _parse(text, fraction_digits=None, zulu_only=False)


def parse_event_timestamp(text: object) -> datetime:
    """A ledger event's timestamp: 0, 3, 6 or 9 fraction digits and a Z (design.md 1.2)."""
    return _parse(text, fraction_digits=frozenset({0, 3, 6, 9}), zulu_only=True)


def truncate_to_ms(moment: datetime) -> datetime:
    return moment.replace(microsecond=moment.microsecond - moment.microsecond % 1000)


def epoch_ms(moment: datetime) -> int:
    """Whole milliseconds since 1970, rounded down; exact for any datetime."""
    return (moment - EPOCH) // ONE_MS


def format_instant(moment: datetime) -> str:
    """UTC with a Z and a fraction of 0, 3 or 6 digits, as tally writes a Postgres timestamp
    (Java's ISO_INSTANT on a value with microsecond precision)."""
    moment = moment.astimezone(UTC)
    text = moment.strftime("%Y-%m-%dT%H:%M:%S")
    micros = moment.microsecond
    if micros == 0:
        return f"{text}Z"
    if micros % 1000 == 0:
        return f"{text}.{micros // 1000:03d}Z"
    return f"{text}.{micros:06d}Z"
