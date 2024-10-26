from calendar import monthrange
import datetime
from datetime import datetime as dt, timedelta
from typing import Generator

from data.constants import YMD

def date_range_monthly(query_start_date: str, query_end_date: str) -> Generator:
    """
    Creates iterator of all year-months within query bounds.
    Params:
        - query_start_date: YMD
        - query_end_date: YMD
    Returns:
        - Generator of YMD, YMD
    """
    query_start_year = from_ymd(query_start_date).year
    query_end_year = from_ymd(query_end_date).year
    query_start_month = from_ymd(query_start_date).month
    query_end_month = from_ymd(query_end_date).month
    query_start_day = from_ymd(query_start_date).day
    query_end_day = from_ymd(query_end_date).day

    for year in range(query_start_year, query_end_year+1):
        year_start_month = query_start_month if year == query_start_year else 1
        year_end_month = query_end_month if year == query_end_year else 12
        for month in range(year_start_month, year_end_month + 1):
            month_start_day = query_start_day if year == query_start_year and month == query_start_month else 1
            month_end_day = query_end_day if year == query_end_year and month == query_end_month else monthrange(year, month)[1]
            month_start_date = datetime(year, month, month_start_day)
            month_end_date = datetime(year, month, month_end_day)
            return (to_ymd(month_start_date), to_ymd(month_end_date))
            # Commented out because the interface is simpler as YMD not ISO.
            # month_start_dt = datetime(year, month, month_start_day).isoformat()
            # month_end_dt = (datetime(year, month, month_end_day) + timedelta(hours=23, minutes=59, seconds=59)).isoformat()
            # yield (month_start_dt, month_end_dt)


def date_range_daily(query_start_date: str, query_end_date: str) -> Generator:
    """
    Creates iterator of all year-month-days within query bounds.
    Params:
        - query_start_dt: YMD
        - query_end_dt: YMD
    Returns:
        - Generator of YMD, YMD
    """
    # In the context this is currently called, the outer monthly loop is only 1 iteration,
    # but it gives it generalizability to handle multiple months.
    for month_start_date, month_end_date in date_range_monthly(query_start_date, query_end_date):
        month_start_date, month_end_date = from_ymd(month_start_date), from_ymd(month_end_date)
        for day in range(month_start_date.day, month_end_date.day+1):
            date = datetime(month_start_date.year, month_start_date.month, day)
            yield (to_ymd(date), to_ymd(date))
            # Commented out because the interface is simpler as YMD
            # day_dt = datetime(month_start_date.year, month_start_date.month, day)
            # day_start_dt = day_dt.isoformat()
            # day_end_dt = (day_dt + timedelta(hours=23, minutes=59, seconds=59)).isoformat()
            # yield (day_start_dt, day_end_dt)


def iso_to_ymd(x: str):
    return dt.strftime(dt.fromisoformat(x), YMD)


def from_ymd(x: str):
    return dt.strptime(x, YMD)


def to_ymd(x: str):
    return dt.strftime(x, YMD)


def ymd_to_iso_sod(x: str):
    return from_ymd(x).isoformat()


def ymd_to_iso_eod(x: str):
    return (from_ymd(x) + timedelta(hours=23, minutes=59, seconds=59)).isoformat()
