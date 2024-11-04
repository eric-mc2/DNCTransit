import pandas as pd
import geopandas as gpd
from shapely.geometry import MultiPoint
from data.constants import LOCAL_CRS

def dms_to_decimal(degrees, minutes, seconds, direction):
    decimal = degrees + minutes / 60 + seconds / 3600
    if direction in ['S', 'W']:  # South and West should be negative
        decimal = -decimal
    return decimal


def project_gdf(gdf, crs=LOCAL_CRS):
    return gdf.to_crs(crs)
        

def meter_to_foot(x: (pd.Series | float)):
    return x * 3.281


def foot_to_meter(x: (pd.Series | float)):
    return x * .3048

def foot_to_mi(x: (pd.Series | float)):
    return x * 1.894e-4

def label_point_dispersion(gdf: gpd.GeoDataFrame, pk: (str | list[str])):
    """Utility function to diagnose relationship between primary key and geometry.
    Params: 
        - pk: dataframe primary key / grouping columns
    """
    # nb: have to break into two steps to bypass sending ints to geoseries constructor
    gdf['diam_ft'] = gdf.groupby(pk)['geometry'].transform(
            lambda x: MultiPoint(x.to_crs(LOCAL_CRS).unique())
        ).apply(lambda x: x.minimum_rotated_rectangle.length)
    gdf['diam_mi'] = gdf['diam_ft'].pipe(foot_to_mi)
    return gdf