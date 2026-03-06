"""Time utilities for run IDs and timestamps."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from secrets import token_hex
import re

WINDOW_PATTERN = re.compile(r"^(?P<count>\d+)(?P<unit>[dwmy])$")


def utc_now() -> datetime:
    """Return timezone-aware current UTC datetime."""
    return datetime.now(UTC)


def utc_iso(dt: datetime | None = None) -> str:
    """Return ISO-8601 UTC timestamp."""
    base = dt or utc_now()
    return base.astimezone(UTC).isoformat()


def new_run_id() -> str:
    """Create stable, sortable run IDs."""
    return f"run_{utc_now().strftime('%Y%m%dT%H%M%SZ')}_{token_hex(3)}"


def parse_window_to_timedelta(window: str) -> timedelta:
    """Convert short window strings like 7d/4w/1m/1y to timedelta."""
    match = WINDOW_PATTERN.match(window.strip().lower())
    if not match:
        raise ValueError(f"Invalid window format: {window}")

    count = int(match.group("count"))
    unit = match.group("unit")
    if unit == "d":
        return timedelta(days=count)
    if unit == "w":
        return timedelta(weeks=count)
    if unit == "m":
        return timedelta(days=30 * count)
    if unit == "y":
        return timedelta(days=365 * count)
    raise ValueError(f"Unsupported window unit: {unit}")
