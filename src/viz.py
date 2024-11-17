import geopandas as gpd
from data.constants import WEB_CRS
from shapely.geometry import shape
import pandas as pd
import plotly.express as px
import json

def _explode_points(df: gpd.GeoDataFrame):
    """Convert LineStrings to arrays of Points to interface with Plotly."""
    lines = gpd.GeoDataFrame(df).explode()
    points = []
    for idx, line in lines[['geometry']].itertuples():
        for pt in shape(line).coords:
            points.append({"idx": idx, "x": pt[0], "y": pt[1]})
        points.append({"idx": idx, "x": None, "y": None})
    points = pd.DataFrame.from_records(points)
    points['geometry'] = gpd.points_from_xy(points['x'], points['y'])
    points = points.merge(lines.drop(columns=['geometry']), left_on='idx', right_index=True, how='left')
    points = gpd.GeoDataFrame(points, crs=df.crs)
    return points
    
def _discretize_color(df, col):
    """Transform continuous values into hex colors."""
    if df.dtypes[col] == 'object':
        cmap = px.colors.qualitative.Prism
        cmap = {x:cmap[i] for i,x in enumerate(df[col].unique())}
        return df.assign(cmap = df[col].map(cmap))
    else:
        cscale = px.colors.sequential.Magma
        cidx = pd.qcut(df['rides'], len(cscale)).cat.codes
        return df.assign(cmap = cidx.map(lambda i: cscale[i]))
    
def plot_lines(df: gpd.GeoDataFrame, color_col: str = None, animation=None) -> None:
    """Create line map in plotly."""
    df = df.pipe(_discretize_color, color_col)
    points = _explode_points(df).to_crs(WEB_CRS)
    # For some reason, saved line_map does not render on website.
    # So we need to just export the line plot and load a basemap in js.
    fig = px.line_geo(points, lat='y', lon='x', 
                      color='cmap',
                      fitbounds='locations',
                      basemap_visible=False,
                      animation_frame=animation,
                      animation_group='route' if animation else None,
                      color_discrete_map='identity')
    fig.update_layout(showlegend=False)
    return fig

def plot_poly(gdf, **kwargs):
    """Create area map in plotly."""
    if 'color' in kwargs:
        gdf = gdf.pipe(_discretize_color, kwargs['color'])
        # HACK: Compute colors explicitly to use in leaflet.
        #       plotly will just use the normal color argument.
    geojson = json.loads(gdf.to_json())
    fig = px.choropleth(gdf, 
                geojson=geojson,
                locations=gdf.index,
                basemap_visible=False,
                fitbounds="locations",
                **kwargs)
    # Set facet names for leaflet
    if 'facet_col' in kwargs:
        for trace in fig['data']:
            trace['name'] = trace['geojson']['features'][0]['properties'][kwargs['facet_col']]
    return fig