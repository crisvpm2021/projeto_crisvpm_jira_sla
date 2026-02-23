from datetime import datetime, timedelta, date
from urllib.request import urlopen, Request
import json


def is_business_day(dt: datetime, holidays: set[date]) -> bool:
    """Business day = Mon-Fri and not a holiday."""
    is_weekday = dt.weekday() < 5  # Mon=0 ... Sun=6
    is_holiday = dt.date() in holidays
    return is_weekday and not is_holiday


def next_midnight(dt: datetime) -> datetime:
    """Return the next day's midnight (00:00)."""
    return dt.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)


def business_hours_between(start: datetime, end: datetime, holidays: set[date]) -> float:
    """
    Calculate business hours between two datetimes:
    - Counts only Mon-Fri
    - Excludes dates in 'holidays'
    - 24h per business day (no business-hours window yet)

    Assumes start/end are timezone-aware (UTC) or both naive consistently.
    """
    if start is None or end is None:
        return 0.0
    if end <= start:
        return 0.0

    total_seconds = 0.0
    current = start

    while current < end:
        slice_end = min(next_midnight(current), end)

        if is_business_day(current, holidays):
            total_seconds += (slice_end - current).total_seconds()

        current = slice_end

    return total_seconds / 3600.0


def get_br_holidays(year: int) -> set[date]:
    """
    Fetch Brazilian national holidays for a given year via public API.
    Uses only Python stdlib (urllib + json).

    API used: https://brasilapi.com.br/api/feriados/v1/{year}
    Returns a set of datetime.date objects.
    """
    url = f"https://brasilapi.com.br/api/feriados/v1/{year}"

    # User-Agent helps avoid some basic blocks
    req = Request(url, headers={"User-Agent": "python-data-engineering-challenge"})
    with urlopen(req, timeout=30) as resp:
        payload = resp.read().decode("utf-8")

    data = json.loads(payload)

    # Response is a list like: [{"date":"2025-01-01","name":"...","type":"national"}, ...]
    holidays = set()
    for item in data:
        d = item.get("date")
        if d:
            holidays.add(date.fromisoformat(d))

    return holidays


def build_holiday_set(years: set[int]) -> set[date]:
    """Fetch holidays for multiple years and combine them into one set."""
    all_holidays = set()
    for y in sorted(years):
        all_holidays |= get_br_holidays(y)
    return all_holidays