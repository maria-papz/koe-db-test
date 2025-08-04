from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import List, Dict
from koe_db.models import Frequency

def format_label(date: datetime, frequency: str) -> str:
    if frequency in [Frequency.MONTHLY, Frequency.BIMONTHLY]:
        return date.strftime("%m-%Y")
    elif frequency == Frequency.QUARTERLY:
        quarter = (date.month - 1) // 3 + 1
        return f"{date.year}-Q{quarter}"
    elif frequency == Frequency.ANNUAL:
        return str(date.year)
    elif frequency == Frequency.WEEKLY:
        return f"Week {date.strftime('%U')} - {date.strftime('%Y')}"
    elif frequency == Frequency.DAILY:
        return date.strftime("%d-%m-%Y")
    else:
        return date.isoformat()

def get_delta(frequency: str) -> relativedelta | timedelta:
    match frequency:
        case Frequency.MINUTE:
            return timedelta(minutes=1)
        case Frequency.HOURLY:
            return timedelta(hours=1)
        case Frequency.DAILY:
            return timedelta(days=1)
        case Frequency.WEEKLY:
            return timedelta(weeks=1)
        case Frequency.BIWEEKLY:
            return timedelta(weeks=2)
        case Frequency.MONTHLY:
            return relativedelta(months=1)
        case Frequency.BIMONTHLY:
            return relativedelta(months=2)
        case Frequency.QUARTERLY:
            return relativedelta(months=3)
        case Frequency.TRIANNUAL:
            return relativedelta(months=4)
        case Frequency.SEMIANNUAL:
            return relativedelta(months=6)
        case Frequency.ANNUAL:
            return relativedelta(years=1)
        case _:
            raise ValueError(f"Invalid or unsupported frequency: {frequency}")

def generate_schedule(start_date: datetime, frequency: str, count: int = 10, backward: bool = False) -> List[Dict]:
    dates = [start_date]
    delta = get_delta(frequency)

    for _ in range(1, count):
        last_date = dates[-1]
        next_date = last_date - delta if backward else last_date + delta
        dates.append(next_date)

    sorted_dates = sorted(dates) if not backward else sorted(dates, reverse=True)
    return [{"date": dt, "label": format_label(dt, frequency)} for dt in sorted_dates]

def is_valid_period(target_date: datetime, start_date: datetime, frequency: str) -> bool:
    delta = get_delta(frequency)
    current = start_date

    # Iterate up to 1000 intervals max
    for _ in range(1000):
        if abs((target_date - current).total_seconds()) < 1:  # exact match
            return True
        if target_date < current:
            return False
        current += delta

    return False  # Prevent infinite loop on bad input
