import geopandas as gpd
import pandas as pd
from os.path import basename
import re
import s3fs
from typing import Generator
from zipfile import ZipFile, BadZipFile

from data.constants import *

class DivvyClient():
    def __init__(self):
        self.s3 = s3fs.S3FileSystem(anon=True)
        
    def get_bucket_paths(self):
        """
        Returns pairs of s3://filepaths and membername.csv in the bucket.
        
        Handles:
        - ignoring irrelevant files in the bucket, like the index.html page
        - ignoring irrelevant member files like system files in the ZIP files
        - including multiple valid CSVs per ZIP like multiple quarters
        - some station_ids are normalized and some aren't
        """
        if not hasattr(self, "bucket_paths_cached") or not self.bucket_paths_cached:
            print("DEBUG: populating bucket paths.")
            self.bucket_paths = list(self._get_bucket_paths())
        return self.bucket_paths


    def _get_bucket_paths(self) -> Generator:
        """
        Worker function for get_bucket_paths(). Caching it since it is mildly expensive.
        """
        s3_paths = [f"s3://{x}" for x in self.s3.glob('divvy-tripdata/*.zip')]
        csv_filter = lambda x: x.endswith('.csv') and 'MACOSX' not in x
        shp_filter = lambda x: x.endswith('.shp.zip') and 'MACOSX' not in x
        trip_filter = lambda x: 'trip' in basename(x.lower()) and csv_filter(x)
        station_filter = lambda x: 'station' in basename(x.lower()) and (csv_filter(x) or shp_filter(x))
        for s3_path in s3_paths:
            with self.s3.open(s3_path, mode='rb') as s3f:
                try:
                    with ZipFile(s3f) as zf:
                        station_path = filter(station_filter, zf.namelist())
                        station_path = sorted(station_path, key=csv_filter)
                        station_path = station_path.pop() if station_path else None
                        for csv_path in filter(trip_filter, zf.namelist()):
                            yield (s3_path, csv_path, station_path)
                        if not any(map(csv_filter, zf.namelist())):
                            print(f"WARNING: Did not find csv in {s3_path}")
                except BadZipFile:
                    print("DEBUG: Skipping BadZipFile: ", s3_path)
        # Now that this is done, set flag on the class instance.
        self.bucket_paths_cached = True

    def _read_trip_file(self, fp):
        """Helper function to unify schema drift."""
        # TODO! We should cache these! This takes ~15s per file via s3
        # But they're also pretty big, so maybe not since we really just want to agg them.
        df = pd.read_csv(fp).pipe(self._trip_schema)
        # Note: station_ids are always a mix of numeric and non-numeric so keep as strings!!
        df['start_time'] = pd.to_datetime(df['start_time'], errors='coerce')
        df['end_time'] = pd.to_datetime(df['end_time'], errors='coerce')
        if 'start_lng' in df.columns:
            df['start_geometry'] = gpd.points_from_xy(df['start_lng'],df['start_lat'],crs=WORLD_CRS)
            df['end_geometry'] = gpd.points_from_xy(df['end_lng'],df['end_lat'],crs=WORLD_CRS)
        return df
    

    def _trip_schema(self, df):
        """Helper function to unify schema drift."""
        return (
            df.rename(columns={
                '01 - Rental Details Rental ID': 'ride_id',
                '01 - Rental Details Local Start Time': 'start_time',
                '01 - Rental Details Local End Time': 'end_time', 
                '01 - Rental Details Bike ID': 'bike_id',
                '01 - Rental Details Duration In Seconds Uncapped': 'tripduration',
                '03 - Rental Start Station ID': 'start_station_id',
                '03 - Rental Start Station Name': 'start_station_name',
                '02 - Rental End Station ID': 'end_station_id',
                '02 - Rental End Station Name': 'end_station_name',
                'User Type': 'user_type',
                'Member Gender': 'gender',
                '05 - Member Details Member Birthday Year': 'birthyear'
                })
            .rename(columns= lambda x: x.lower())
            .rename(columns = {
                'from_lng': 'start_lng',
                'from_lat': 'start_lat',
                'to_lng': 'end_lng',
                'to_lat': 'end_lat',
                'trip_id': 'ride_id',
                'from_station_id': 'start_station_id',
                'to_station_id': 'end_station_id',
                'from_station_name': 'start_station_name',
                'to_station_name': 'end_station_name',
                'starttime': 'start_time',
                'stoptime': 'end_time',
                'stop_time': 'end_time',
                'started_at': 'start_time',
                'ended_at': 'end_time',
                'bikeid': 'bike_id',
                'tripduration': 'trip_duration',
                'usertype': 'user_type',
                'member_casual': 'user_type',
                'duration': 'trip_duration',
                'birthday': 'birthyear',
            }))

    def s3_bike_stations(self) -> list[gpd.GeoDataFrame]:
        """
        Reads normalized bike station files from S3 into memory.
        """
        dfs = []
        for zip_path, _, station_path in filter(lambda x: x[2], self.get_bucket_paths()):
            with (self.s3.open(zip_path, mode='rb') as s3f, ZipFile(s3f) as zf, zf.open(station_path) as stationf):
                df = self.read_station_file(stationf).assign(vintage=basename(zip_path))
                gdf = self.s3_point_gdf(df, "longitude","latitude","geometry")
                dfs.append(gdf)
        return dfs

    def s3_bike_trips(self, min_year: int, max_year: int) -> Generator:
        """
        Reads bike trips from S3 into memory.
        
        Note: for simplicity we're not pattern matching on months or days,
               mostly because some files are monthly, some are quarterly, some are semi-annually
               which I don't feel like parsing. 
            
        Returns:
            - Generator of DataFrames
        """
        for zip_path, trip_path, station_path in self.get_bucket_paths():
            # Identifying ride data files (not station or irrelevant files)
            pats = [r"(\d{4}).*-divvy-tripdata.zip", r"Divvy_.*Trips_(\d{4}).*.zip"]
            matches = filter(None, map(lambda z: re.search(z, zip_path), pats))
            # Extract the year of this data file
            year = int(next(map(lambda y: y.group(1), matches)))
            # If the data file is within our date range, proceed.
            if min_year <= year and year <= max_year:
                with self.s3.open(zip_path, mode='rb') as s3f, ZipFile(s3f) as zf, zf.open(trip_path) as tripf:
                    df = self._read_trip_file(tripf).assign(vintage=basename(zip_path))
                    yield df


    def read_station_file(self, fp: str) -> pd.DataFrame: 
        """Helper function to unify schema drift."""
        if fp.name.endswith(".csv"):
            df = pd.read_csv(fp).pipe(self._station_schema)
        else:
            df = gpd.read_file(fp).pipe(self._station_schema)
        # Note: station_id is mix numeric and non-numeric ids so don't coerce!!
        keep_cols = ['station_id','name','latitude','longitude','geometry']
        return df.filter(keep_cols)


    def _station_schema(self, df):
        """Helper function to unify schema drift."""
        return (
            df.rename(columns= lambda x: x.lower())
            .rename(columns = {
                "lat": "latitude",
                "long": "longitude",
                "id_list": "station_id",
                "id": "station_id",
                "online date": "online_date"
            }))


    def s3_point_gdf(self, df, lng_col, lat_col, loc_col) -> gpd.GeoDataFrame:
        """Helper function to compute projected geometries."""
        crs = df.crs if isinstance(df, gpd.GeoDataFrame) else WORLD_CRS
        if loc_col not in df.columns:
            df = df.assign(**{loc_col: gpd.points_from_xy(df[lng_col], df[lat_col], crs=crs)})
        return gpd.GeoDataFrame(df, geometry=loc_col, crs=crs)