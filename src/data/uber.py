import os
import pandas as pd
from shapely.geometry import shape
from requests.exceptions import ReadTimeout
from urllib3.exceptions import TimeoutError

from data.constants import *
from data.cta import CTAClient
from data.datemath import (date_range_daily, date_range_monthly, 
                            ymd_to_iso_eod, ymd_to_iso_sod, from_ymd)

class UberClient(CTAClient):
    """
    Exposes rideshare-specific transformations on data accessed through CTA Socrata API.
    """
    def __init__(self, timeout:int):
        super().__init__(timeout=timeout)


    def soda_get_uber(self, select: str, where_start: str, where_end:str, pickup=True, **kwargs):
        """
        Paginates soda_get_all over query months which is the largest timespan 
        the socrata endpoint can seem to handle.
        Params:
            - where_start: YMD
            - where_end: YMD
        """
        month_dfs = []
        for month_start_date, month_end_date in date_range_monthly(where_start, where_end):
            try:
                month_df = self._get_uber(select, month_start_date, month_end_date, pickup, **kwargs)
            except (TimeoutError, ReadTimeout):
                day_dfs = []
                for day_start_date, day_end_date in date_range_daily(month_start_date, month_end_date):
                    day_dfs.append(self._get_uber(select, day_start_date, day_end_date, pickup, **kwargs))
                month_df = pd.concat(day_dfs, ignore_index=True)
            month_dfs.append(month_df)
        rides = pd.concat(month_dfs, ignore_index=True)
        return rides


    def _get_uber(self, select: str, start_date: str, end_date: str, pickup: bool, **kwargs):
        """
        Makes single network call to get uber data, or retrieves cached version.
        Params:
            - start_date: start YMD
            - end_date: end YMD (automatically converts to 11:59PM!)
        Raises:
            - TimeoutError, ReadTimeout: if query is too big

        """
        dfs = self._get_cached_uber(start_date, end_date, pickup)
        if len(dfs) == 1 and dfs[0] is not None:
            # print(f"INFO: Found cached monthly file {start_date} -> {end_date}")
            return dfs[0] # Monthly
        elif all(map(lambda x: x is not None, dfs)):
            # print(f"INFO: Found {len(dfs)}/{len(dfs)} files for {start_date} -> {end_date}")
            dfs = pd.concat(dfs, ignore_index=True)
            return dfs
        elif len(dfs) > 1:
            num_cached = sum(map(lambda x: x is not None, dfs))
            print(f"INFO: Found {num_cached}/{len(dfs)} files for {start_date} -> {end_date}")
            # Basically short-circuiting this call because we have partial results 
            # so we know the full query would time out if re-tried.
            raise ReadTimeout("Force caller to split into daily queries")
        else:
            # Need monthly or daily file. Continue to query.
            pass
        
        dt_col = "trip_start_timestamp" if pickup else "trip_end_timestamp"
        start_dt, end_dt = ymd_to_iso_sod(start_date), ymd_to_iso_eod(end_date)
        where_dates = f"{dt_col} between '{start_dt}' and '{end_dt}'"
        print("INFO: GET where " + where_dates)
        df = self.soda_get_all(UBER_RIDERSHIP_TABLE, 
                               select=select, 
                               where=where_dates, 
                               **kwargs) \
                .pipe(self._fix_uber_schema)

        cache_file = self._cached_filename(start_date, end_date, pickup)
        df.to_csv(cache_file, index=False)
        return df


    def _get_cached_uber(self, start_date: str, end_date: str, pickup: bool) -> list[pd.DataFrame]:
        """
        Loads and concatenates monthly or daily cached files.
        Returns None if the entire date range is NOT cached.
        Params:
            - start_date: YMD
            - end_date: YMD
        """

        # Right now this is only designed to cache monthly or daily files. Not multi-month.
        # Assumes the query will not span more than one month!
        assert from_ymd(start_date).year == from_ymd(end_date).year \
            and from_ymd(start_date).month == from_ymd(end_date).month
        month_start_date, month_end_date = next(date_range_monthly(start_date, end_date))
        
        # If the monthly-level file exists, return that
        cache_file = self._cached_filename(month_start_date, month_end_date, pickup)
        if os.path.exists(cache_file):
            return [pd.read_csv(cache_file)]
        
        # Otherwise concatenate together the daily files
        day_dfs = []
        for date, _ in date_range_daily(month_start_date, month_end_date):
            cache_file = self._cached_filename(date, date, pickup)
            day_df = pd.read_csv(cache_file) if os.path.exists(cache_file) else None
            day_dfs.append(day_df)
        return day_dfs
        


    def _fix_uber_schema(self, df):
        if 'rides' in df.columns:
            df['rides'] = pd.to_numeric(df['rides'], 'coerce')
        return df


    def _cached_filename(self, start_date: str, end_date: str, pickup: bool):
        """
        Resolves filepath to uber data.
        Params:
            - start_date: YMD
            - end_date: YMD
        """
        pickup = "pickup" if pickup else "dropoff"
        data_dir = os.path.join(DATA_FOLDER, "raw")
        return os.path.join(data_dir, f"uber-{pickup}-{start_date}--{end_date}.csv")
