import numpy as np
import pandas as pd
import geopandas as gpd

def coalesce(df: pd.DataFrame, left: str, right: str, coalesced: str):
    """
    Like SQL coalesce. Takes non-null values of left, or right.
    Params:
        - df to operate on
        - left column
        - right column
        - coalesced new column (can equal left or right)
    """
    values = np.where(df[left].notna(), df[left], df[right])
    to_drop = [right,left] + [coalesced] if coalesced in df.columns else []
    if isinstance(df, gpd.GeoDataFrame):
        if df.active_geometry_name in [left,right,coalesced]:
            return (df.drop(columns=to_drop)
                    .assign(**{coalesced: values})
                    .set_geometry(coalesced, crs=df.crs))
    return df.drop(columns=to_drop).assign(**{coalesced: values})
