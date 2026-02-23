from datetime import datetime, timedelta


def is_business_day(dt: datetime) -> bool:
    """Monday=0 ... Sunday=6. Business days are Mon-Fri."""
    return dt.weekday() < 5


def next_midnight(dt: datetime) -> datetime:
    """Return the next day's midnight (00:00)."""
    return (dt.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1))


def business_hours_between(start: datetime, end: datetime) -> float:
    """
    Calculate business hours between two datetimes.
    Rule (v2): count only Mon-Fri hours, 24h per business day.
    Weekends (Sat/Sun) contribute 0 hours.

    Assumes start <= end and both are timezone-aware (UTC) or both naive consistently.
    """
    if start is None or end is None:
        return 0.0
    if end <= start:
        return 0.0

    total_seconds = 0.0
    current = start

    while current < end:
        # Determine the end of the current day slice
        day_end = min(next_midnight(current), end)

        # Add time only if it's a business day
        if is_business_day(current):
            total_seconds += (day_end - current).total_seconds()

        current = day_end

    return total_seconds / 3600.0