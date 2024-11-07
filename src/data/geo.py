import pandas as pd
import geopandas as gpd
import numpy as np
from shapely import from_wkb
from shapely.geometry import MultiPoint, Point
from data.constants import LOCAL_CRS
from functools import partial

def dms_to_decimal(degrees, minutes, seconds, direction):
    decimal = degrees + minutes / 60 + seconds / 3600
    if direction in ['S', 'W']:  # South and West should be negative
        decimal = -decimal
    return decimal


def project_gdf(gdf, crs=LOCAL_CRS):
    return gdf.to_crs(crs)
        
def wkb_geom(gdf, geo_col):
    return gdf.assign(
        **{geo_col: gpd.GeoSeries(gdf[geo_col].apply(from_wkb), crs=gdf.crs)})

def meter_to_foot(x: (pd.Series | float)):
    return x * 3.281


def foot_to_meter(x: (pd.Series | float)):
    return x * .3048

def foot_to_mi(x: (pd.Series | float)):
    return x * 1.894e-4

def label_point_dispersion(gdf: gpd.GeoDataFrame, 
                           pk: (str | list[str]), 
                           geom: str = 'geometry',
                           metric: str = 'std'):
    """Utility function to diagnose relationship between primary key and geometry.
    Params: 
        - pk: dataframe primary key / grouping columns
    """
    disp_func = partial(_dispersion, metric=metric)
    diam_ft = gdf.groupby(pk)[geom].transform(disp_func).y
    diam_mi = diam_ft.pipe(foot_to_mi)
    return gdf.assign(diam_ft = diam_ft, diam_mi = diam_mi)

def _dispersion(xs: gpd.GeoSeries, metric: str = 'std') -> gpd.GeoSeries:
    """
    Fast approximation of diameter of stdev of blob of points.
    Params:
        - xs: the points
        - metric: aggregation func
    Note:
        Agg func mean -> MAD
        Agg func std is more conservative by emphasizing larger deviations.
        Agg func max -> most conservative by highlighting mis-coded points.
    """
    # nb: return type is geoseries bc transformations preserve series type
    #       so geopandas complains when we transform geometry col -> float array
    xs = xs.drop_duplicates().to_crs(LOCAL_CRS)
    c = MultiPoint(xs.values).centroid
    agg_func = getattr(np, metric)
    return Point(0, agg_func(xs.distance(c)) * 2)  # 2x roughly the radius