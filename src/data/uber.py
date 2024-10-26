import os
import pandas as pd
from requests.exceptions import ReadTimeout
from shapely.geometry import shape
from urllib3.exceptions import TimeoutError

from data.constants import *
from data.cta import CTAClient
from data.datemath import date_range_daily, date_range_monthly, ymd_to_iso_eod, ymd_to_iso_sod

class UberClient(CTAClient):
    """
    Exposes rideshare-specific transformations on data accessed through CTA Socrata API.
    """
    def __init__(self, timeout):
        super().__init__(timeout=timeout)


    def soda_get_uber(self, query_start_date: str, query_end_date:str, pickup=True, **kwargs):
        """
        Paginates soda_get_all over query months which is the largest timespan 
        the socrata endpoint can seem to handle.
        Params:
            - query_start_date: YMD
            - query_end_date: YMD
        """
        month_dfs = []
        for month_start_date, month_end_date in date_range_monthly(query_start_date, query_end_date):
            try:
                month_df = self._get_uber(month_start_date, month_end_date, pickup, **kwargs)
            except (TimeoutError, ReadTimeout):
                day_dfs = []
                for day_start_date, day_end_date in date_range_daily(month_start_date, month_end_date):
                    day_dfs.append(self._get_uber(day_start_date, day_end_date, pickup, **kwargs))
                month_df = pd.concat(day_dfs, ignore_index=True)
            month_dfs.append(month_df)
        rides = pd.concat(month_dfs, ignore_index=True)
        return rides


    def _get_uber(self, start_date: str, end_date: str, pickup: bool, **kwargs):
        """
        Makes single network call to get uber data, or retrieves cached version.
        Params:
            - start_date: start YMD
            - end_date: end YMD (automatically converts to 11:59PM!)
        Raises:
            - TimeoutError, ReadTimeout: if query is too big

        """
        df = self._get_cached_uber(start_date, end_date, pickup)
        if df is not None:
            return df
        
        dt_col = "trip_start_timestamp" if pickup else "trip_end_timestamp"
        start_dt, end_dt = ymd_to_iso_sod(start_date), ymd_to_iso_eod(end_date)
        where_dates = f"{dt_col} between '{start_dt}' and '{end_dt}'"
        print("DEBUG: GET where " + where_dates)
        df = self.soda_get_all(UBER_RIDERSHIP_TABLE, where=where_dates, **kwargs) \
                .pipe(self._fix_uber_schema)

        cache_file = self._cached_filename(start_date, end_date, pickup)
        df.to_csv(cache_file, index=False)
        return df


    def _get_cached_uber(self, start_date: str, end_date: str, pickup: bool):
        """
        Loads and concatenates monthly or daily cached files.
        Returns None if the entire date range is NOT cached.
        Params:
            - start_date: YMD
            - end_date: YMD
        """

        # Right now this is only designed to cache monthly or daily files. Not multi-month.
        # Assumes the query will not span more than one month!
        assert start_date.year == end_date.year and start_date.month == end_date.month
        month_start_date, month_end_date = next(date_range_monthly(start_date, end_date))
        
        # If the monthly-level file exists, return that
        cache_file = self._cached_filename(month_start_date, month_end_date, pickup)
        if os.path.exists(cache_file):
            print("DEBUG: Found cached monthly file " + cache_file)
            return pd.read_csv(cache_file)
        
        # Otherwise concatenate together the daily files
        day_dfs = []
        for date, _ in date_range_daily(month_start_date, month_end_date):
            cache_file = self._cached_filename(date, date, pickup)
            day_df = pd.read_csv(cache_file) if os.path.exists(cache_file) else None
            day_dfs.append(day_df)
        
        print("DEBUG: found {}/{} files for {} -> {}".format(
            sum(map(lambda x: x is not None, day_dfs)), len(day_dfs), start_date, end_date
        ))
        # Return the concatenated dfs if we have ALL of them. Otherwise return error state.
        if all(lambda x: x is not None, day_dfs):
            return pd.concat(day_dfs, ignore_index=True)
        else:
            return None


    def _fix_uber_schema(self, df):
        renamer = {"pickup_centroid_location": "start_point",
                "dropoff_centroid_location": "end_point",
                "trip_end_timestamp": "end_date",
                "trip_start_timestamp": "start_date"}
        df = df.rename(columns=renamer)
        if 'rides' in df.columns:
            df['rides'] = pd.to_numeric(df['rides'], 'coerce')
        if 'start_point' in df.columns:
            df['start_point'] = df['start_point'].apply(shape)
        if 'end_point' in df.columns:
            df['end_point'] = df['end_point'].apply(shape)
        if 'start_date' in df.columns:
            df['start_date'] = pd.to_datetime(df['start_date'],'coerce').dt.date
        if 'end_date' in df.columns:
            df['end_date'] = pd.to_datetime(df['end_date'],'coerce').dt.date
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
