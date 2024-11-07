import os
from pathlib import Path
from datetime import datetime as dt
import pyproj

# Rides
TOTAL_RIDERSHIP_TABLE = "6iiy-9s97" # https://data.cityofchicago.org/Transportation/CTA-Ridership-Daily-Boarding-Totals/6iiy-9s97
L_RIDERSHIP_TABLE = "5neh-572f" # https://data.cityofchicago.org/Transportation/CTA-Ridership-L-Station-Entries-Daily-Totals/5neh-572f
BUS_RIDERSHIP_TABLE = "jyb9-n7fm" # https://data.cityofchicago.org/Transportation/CTA-Ridership-Bus-Routes-Daily-Totals-by-Route/jyb9-n7fm
DIVVY_RIDERSHIP_TABLE = "fg6s-gzvg" # https://data.cityofchicago.org/Transportation/Divvy-Trips/fg6s-gzvg
DIVVY_SUB_RIDERSHIP_TABLE = "4am4-35ir" # https://data.cityofchicago.org/Transportation/Divvy-Trips-Subscriber-Only/4am4-35ir
UBER_RIDERSHIP_TABLE = "n26f-ihde" # https://data.cityofchicago.org/resource/n26f-ihde

# Stations
L_STATIONS_TABLE = "8pix-ypme" # https://data.cityofchicago.org/Transportation/CTA-System-Information-List-of-L-Stops/8pix-ypme
DIVVY_STATIONS_TABLE = "bbyy-e7gq" # https://data.cityofchicago.org/Transportation/Divvy-Bicycle-Stations/bbyy-e7gq
DIVVY_STATIONS_GBFS = "https://gbfs.lyft.com/gbfs/2.3/chi/en/station_information.json"
BUS_ROUTES_TABLE = "6uva-a5ei" # https://data.cityofchicago.org/Transportation/CTA-Bus-Routes/6uva-a5ei
BUS_STOPS_TABLE = "pxug-u72f" # https://data.cityofchicago.org/Transportation/CTA-Bus-Stops-Shapefile/pxug-u72f
BUS_STOPS_TABLE = "https://data.cityofchicago.org/download/pxug-u72f/application/x-zip-compressed"
BUILDINGS_TABLE = "syp8-uezg" # https://data.cityofchicago.org/Buildings/buildings/syp8-uezg
TRACT_TABLE = "https://data.cityofchicago.org/api/geospatial/5jrd-6zik?method=export&format=GeoJSON"
COMM_AREA_TABLE = "https://data.cityofchicago.org/api/geospatial/cauq-8yn6?method=export&format=GeoJSON"
CHI_BOUNDARY_FILE = "https://data.cityofchicago.org/api/geospatial/qqq8-j68g?method=export&format=GeoJSON"

UNITED_CENTER = ((41,52,50,"N"), (87,40,27,"W")) # lat/lng
MCCORMICK_PLACE = ((41,51,7,"N"), (87,36,58,"W"))
OHARE_CENTROID = ((41,58,43,"N"), (87,54,17,"W"))
MIDWAY_CENTROID = ((41,47,10,"N"), (87,45,9,"W"))

YMD = "%Y-%m-%d"
DNC_START = "2024-08-19"
DNC_END = "2024-08-22"
ERAS_START = "2023-06-02"
ERAS_END = "2023-06-04"
# TODO!
# # Using the Taylor Swift Eras tour as a stand-in for the DNC since transit data isn't updated to August yet.
# DNC_START = ERAS_START
# DNC_END = ERAS_END
DNC_START_ISO = dt.strptime(DNC_START, YMD).isoformat()
DNC_END_ISO = dt.strptime(DNC_END, YMD).replace(hour=23, minute=59, second=59).isoformat()
dnc_isoc = dt.strptime(DNC_START, YMD).isocalendar()
DNC_ISO_WEEK = "{}-{}".format(dnc_isoc.year, str(dnc_isoc.week).rjust(2,'0'))

# I'm going to consider Phase 5 as the start point for the new normal.
# This is "Illinois Restored", gatherings of 50+, including festivals, etc.
COVID = {"TIER_3": "2021-01-02",
         "TIER_1": "2021-01-19",
         "PHASE_4": "2021-02-02",
         "PHASE_5": "2021-06-11"}

LOCAL_CRS = pyproj.CRS("EPSG:3435") # NAD83 / Illinois East (ftUS)
WORLD_CRS = pyproj.CRS("EPSG:4326") # WGS84
WEB_CRS = pyproj.CRS("EPSG:3857") # Web Mercador

PROJECT_ROOT = str(Path(__file__).resolve().parents[2])  # assuming this file is at depth=2 from root
DATA_FOLDER = os.path.join(PROJECT_ROOT, "data")