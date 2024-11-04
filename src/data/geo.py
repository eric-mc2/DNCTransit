from data.constants import LOCAL_CRS
import pandas as pd

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