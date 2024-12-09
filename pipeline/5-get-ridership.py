# %%
from datetime import datetime as dt
import geopandas as gpd
import pandas as pd
from tqdm import tqdm

from data.constants import (L_RIDERSHIP_TABLE, BUS_RIDERSHIP_TABLE)
from data.cta import CTAClient
from data.divvy import DivvyClient
from data.uber import UberClient
from data.datemath import iso_to_ymd, is_iso, to_ymd

# %%
train_rides_out = "../data/raw/train_rides.csv"
bus_rides_out = "../data/raw/bus_rides.csv"
bike_rides_out = "../data/raw/bike_rides.geoparquet"
uber_rides_out = "../data/raw/uber_rides.parquet"

# %%
cta_client = CTAClient(60)
divvy_client = DivvyClient()
uber_client = UberClient(60*15)

# %% [markdown]
# # Pipeline In
# 
# (None)

# %% [markdown]
# # Define Data Scope

# %% [markdown]
# Some of these tables are rather large so we need to make good choices about
# what to pull in. We should abstract any logic that we might need to re-do
# if we want to pull in additional dates, and cache anything that takes a while to load.

# %% [markdown]
# Looking ahead, we use models with -1 week, -1 month, and -1 YTD, at daily granularity.
# 
# Therefore we will pull in data from JANUARY 1, 2024 through AUGUST 31, 2024.
# 
# Note: we want to finish the whole month of August to ensure we have a FULL 
# week of data for the DNC. Otherwise we may mis-infer a weekly/monthly effect of DNC on ridership
# when actually we just mechanically omitted some days!

# %%
data_start_iso = dt(2024, 1, 1).isoformat()
data_end_iso = dt(2024, 8, 31, 23, 59, 59).isoformat()

# %% [markdown]
# # Train Rides

# %%
train_rides = cta_client.soda_get_all(L_RIDERSHIP_TABLE, 
                            select="station_id,date,daytype,rides",
                            where=f"date between '{data_start_iso}' and '{data_end_iso}'")

# %% [markdown]
# # Bus Rides

# %%
bus_rides = cta_client.soda_get_all(BUS_RIDERSHIP_TABLE, 
                            select="route,date,daytype,rides",
                            where=f"date between '{data_start_iso}' and '{data_end_iso}'")

# %% [markdown]
# # Bike Rides

# %% [markdown]
# The divvy ridership are at the ride granularity, so we need to aggregate to station-level.

# %%
def agg_ridership(trips: pd.DataFrame):
    """
    Get counts by station and date.
    """
    trips['start_date'] = trips['start_time'].dt.date.apply(to_ymd)
    trips['end_date'] = trips['end_time'].dt.date.apply(to_ymd)
    id_cols = ['station_id','station_name','date','vintage'] 
    id_cols += ['geometry'] if any('geometry' in x for x in trips.columns) else []
    start_rides = trips.rename(columns=lambda x: x.replace('start_','')) \
                    .groupby(id_cols, as_index=False).size() \
                    .rename(columns={'size': 'start_rides'})
    end_rides = trips.rename(columns=lambda x: x.replace('end_','')) \
                    .groupby(id_cols, as_index=False).size() \
                    .rename(columns={'size': 'end_rides'})
    rides = start_rides.merge(end_rides, how='outer')
    rides['rides'] = rides['start_rides'].fillna(0) + rides['end_rides'].fillna(0)
    return rides

# %%
# Takes ~2m to run
bike_rides = divvy_client.s3_bike_trips(dt.fromisoformat(data_start_iso).year, 
                                        dt.fromisoformat(data_end_iso).year)
bike_rides = map(agg_ridership, bike_rides)
bike_rides = pd.concat(list(tqdm(bike_rides)), ignore_index=True)
bike_rides = bike_rides.loc[(bike_rides['date'] >= iso_to_ymd(data_start_iso)) & \
                            (bike_rides['date'] <= iso_to_ymd(data_end_iso))]

# %%
bike_rides = gpd.GeoDataFrame(bike_rides, geometry='geometry')

# %%
# Note, according to https://data.cityofchicago.org/Transportation/Divvy-Bicycle-Stations/bbyy-e7gq/about_data
# each station contains multiple bike docks. Nevertheless, the station location
# has crap accuracy and precision. Nominally there are 900k unique station points.
# From the normalized stations, we expect ~3.7k or fewer stations.
# We'll address this in a subsequent notebook.

# %% [markdown]
# # Uber Rides

# %%
# TODO: 
# Need to consolidate by tract and by community area files.
# Also the community area ridership we have isn't exclusive of known tracts
# So we may want to query that if we're trying to disagg community areas.

from time import sleep

from requests.exceptions import ReadTimeout
from urllib3.exceptions import TimeoutError
done = False
while not done:
    try:
        uber_pickups = uber_client.soda_get_uber(select="""
                                    date_trunc_ymd(trip_start_timestamp) as start_date, 
                                    pickup_community_area,
                                    count(trip_id) as rides
                                    """,
                                    where_start=iso_to_ymd(data_start_iso), 
                                    where_end=iso_to_ymd(data_end_iso), 
                                    group="start_date, pickup_community_area",
                                    pickup=True)
        uber_dropoffs = uber_client.soda_get_uber(select="""
                                    date_trunc_ymd(trip_end_timestamp) as end_date, 
                                    dropoff_community_area,
                                    count(trip_id) as rides
                                    """,
                                    where_start=iso_to_ymd(data_start_iso), 
                                    where_end=iso_to_ymd(data_end_iso), 
                                    group="end_date, dropoff_community_area",
                                    pickup=False)
        done = True
    except (ReadTimeout, TimeoutError) as err:
        print("Read timeout. Retrying in 60s")
        sleep(60)

# %% [markdown]
# During the SODA query we allow tract to be null because it's unlikely 
# both the pickup and dropoff tracts will be null. Our modeling approach
# is to count "rides" regardless of pickup or dropoff, meaning we sum together
# pickups and dropoffs. Therefore, we can drop pickups at unknown locations
# necessarily dropping the corresponding dropoff, and vice versa.
# 
# (Actually we could have done this in the query too, which we broke up 
# separately for pickup and drop-offs for performance reasons.)

# %%
uber_pickups = uber_pickups.rename(columns={'start_date':'date', 'pickup_census_tract':'tract', 'rides':'start_rides'})
uber_dropoffs = uber_dropoffs.rename(columns={'end_date':'date', 'dropoff_census_tract':'tract', 'rides':'end_rides'})
uber_pickups['tract'] = pd.to_numeric(uber_pickups['tract'], 'coerce')
uber_dropoffs['tract'] = pd.to_numeric(uber_dropoffs['tract'], 'coerce')

# %%
# TODO: XXX:
# Unbelievably, 1/3 of all rides are obfuscated not even at the tract level. :(((((
pd.concat([
    uber_pickups.assign(nulltract = uber_pickups.tract.isna()).groupby('nulltract')['start_rides'].sum(),
    uber_dropoffs.assign(nulltract = uber_dropoffs.tract.isna()).groupby('nulltract')['end_rides'].sum()],
    axis=1)

# %%
# TODO: XXX:
pd.concat([
    uber_pickups.assign(nullcomm = uber_pickups.pickup_community_area.isna()).groupby('nullcomm')['start_rides'].sum(),
    uber_dropoffs.assign(nullcomm = uber_dropoffs.dropoff_community_area.isna()).groupby('nullcomm')['end_rides'].sum()],
    axis=1)

# %%
# TODO: Drop null tracts now or later?

# Drop pickups or dropoffs that have obfuscated the tract.
pickup_mask = uber_pickups.tract.notna()
dropoff_mask = uber_dropoffs.tract.notna()
print("Dropping {} pickups ({:.1%} of rows, {:.1%} of rides)".format(
    sum(~pickup_mask),
    sum(~pickup_mask) / len(uber_pickups),
    uber_pickups[~pickup_mask]['start_rides'].sum() / uber_pickups['start_rides'].sum(),
))
print("Dropping {} dropoffs ({:.1%} of rows, {:.1%} of rides)".format(
    sum(~dropoff_mask),
    sum(~dropoff_mask) / len(uber_dropoffs),
    uber_dropoffs[~dropoff_mask]['end_rides'].sum() / uber_dropoffs['end_rides'].sum(),
))

# %%
# TODO: Drop null tracts now or later?
pd.concat([
    uber_pickups.assign(nulltract = uber_pickups.tract.isna()).groupby('nulltract')['start_rides'].sum(),
    uber_dropoffs.assign(nulltract = uber_dropoffs.tract.isna()).groupby('nulltract')['end_rides'].sum()],
    axis=1)

# %%
uber_rides = uber_pickups.merge(uber_dropoffs, how='outer')
uber_rides['rides'] = uber_rides['start_rides'].fillna(0) + uber_rides['end_rides'].fillna(0)
uber_rides['date'] = uber_rides['date'].apply(lambda x: iso_to_ymd(x) if is_iso(x) else x)

# %% [markdown]
# # Pipeline out

# %%
train_rides.to_csv(train_rides_out, index=False)
bus_rides.to_csv(bus_rides_out, index=False)
bike_rides.to_parquet(bike_rides_out, index=False)
uber_rides.to_parquet(uber_rides_out, index=False)


