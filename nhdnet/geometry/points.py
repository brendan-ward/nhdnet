import geopandas as gp
import numpy as np
from shapely.geometry import Point


def create_points(df, x_column, y_column, crs):
    """Create a GeoDataFrame from pandas DataFrame
    
    Parameters
    ----------
    df : pandas DataFrame
    x_column : str
        column containing x values
    y_colummn : str
        column containing y values
    crs : geopandas CRS object
        CRS of points
    
    Returns
    -------
    geopandas.GeoDataFrame
    """

    geometry = [Point(xy) for xy in zip(df[x_column], df[y_column])]
    return gp.GeoDataFrame(df, geometry=geometry, crs=crs)


def remove_duplicates(df, tolerance):
    """Reduce points that are within tolerance of each other to the first record.

    WARNING: no evaluation of the underlying attribute values is performed, 
    only spatial de-duplication.
    
    Parameters
    ----------
    df : GeoDataFrame
    tolerance : number
        distance (in projection units) within which all points are dropped except the first.
    """

    temp = df[["geometry"]].copy()
    temp["x"] = (temp.geometry.x / tolerance).apply(np.floor).astype("int") * tolerance
    temp["y"] = (temp.geometry.y / tolerance).apply(np.floor).astype("int") * tolerance
    clean = temp.drop_duplicates(subset=["x", "y"], keep="first")
    return df.loc[df.index.isin(clean.index)].copy()


def mark_duplicates(df, tolerance):
    """mark points that are within tolerance of each other to the first record.

    WARNING: no evaluation of the underlying attribute values is performed, 
    only spatial de-duplication.
    
    Parameters
    ----------
    df : GeoDataFrame with columns 
        "duplicate" (True if a duplicate EXCEPT first of each duplicate)
        "dup_group" id of each set of duplicates INCLUDING the first of each duplicate
        "dup_count" number of duplicates per duplicate group
    tolerance : number
        distance (in projection units) within which all points are dropped except the first.
    """

    df["temp_x"] = (df.geometry.x / tolerance).apply(np.floor).astype("int") * tolerance
    df["temp_y"] = (df.geometry.y / tolerance).apply(np.floor).astype("int") * tolerance

    # assign duplicate group ids
    grouped = df.groupby(["temp_x", "temp_y"])
    df["dup_group"] = grouped.grouper.group_info[0]
    df = df.join(grouped.size().rename("dup_count"), on=["temp_x", "temp_y"])
    dedup = df.drop_duplicates(subset=["dup_group"], keep="first")
    df["duplicate"] = False
    df.loc[~df.index.isin(dedup.index), "duplicate"] = True

    return df.drop(columns=["temp_x", "temp_y"])


def add_lat_lon(df):
    """Add lat and lon columns to dataframe in WGS84 coordinates
    
    Parameters
    ----------
    df : GeoDataFrame
    
    Returns
    -------
    GeoDataFrame with lat, lon columns added
    """
    geo = df[["geometry"]].to_crs(epsg=4326)
    geo["lat"] = geo.geometry.y.astype("float32")
    geo["lon"] = geo.geometry.x.astype("float32")
    return df.join(geo[["lat", "lon"]])

