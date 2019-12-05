import pandas as pd
import geopandas as gp
import numpy as np
from shapely.geometry import Point


def to2D(geometry):
    """Flatten a 3D point to 2D.

    Parameters
    ----------
    geometry : Point
        Input 3D geometry

    Returns
    -------
    Point
        Output 2D geometry
    """

    return Point(geometry.x, geometry.y)


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

    df["temp_x"] = (df.geometry.x / tolerance).round().astype("int") * tolerance
    df["temp_y"] = (df.geometry.y / tolerance).round().astype("int") * tolerance

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


def count_nearby(df, distance):
    """Return count of points that are within a distance of each point.
    This is symmetric, every original point will have a count of distances to
    all other points.

    Parameters
    ----------
    df : GeoDataFrame
    distance : number
        radius within which to count nearby points

    Returns
    -------
    GeoSeries
        count of points within distance of each original point, based on original index of GeoDataFrame
    """

    print("Creating buffers...")
    buffers = df.copy()
    buffers.geometry = buffers.geometry.buffer(distance)

    print("Creating spatial indices for join...")
    buffers.sindex
    df.sindex

    print("Joining buffers back to points...")
    joined = gp.sjoin(buffers, df, op="intersects")
    joined = joined.loc[joined.index != joined.index_right]
    return joined.groupby(level=0).index_right.size().rename("nearby")


def snap_to_point(df, target_points, tolerance=100, sindex=None):
    """
    Attempt to snap a line to the nearest line, within tolerance distance.

    Lines must be in a planar (not geographic) projection and points
    must be in the same projection.

    Parameters
    ----------
    df : GeoPandas.DataFrame
        points to snap
    target_points : GeoPandas.DataFrame
        points to snap against
    tolerance : int, optional (default: 100)
        maximum distance between target_point and point that can still be snapped

    Returns
    -------
    geopandas.GeoDataFrame
        output data frame containing:
        * all columns from points except geometry
        * geometry: snapped geometry
        * snap_dist: distance between original point and snapped location
        * nearby: number of nearby lines within tolerance
        * any columns joined from lines
    """

    if sindex is None:
        sindex = target_points.sindex
        # Note: the spatial index is ALWAYS based on the integer index of the
        # geometries and NOT their index

    target_columns = target_points.columns.to_list()

    # generate a window around each point
    window = df.bounds + [-tolerance, -tolerance, tolerance, tolerance]
    # get a list of the point ordinal line indexes (integer index, not actual index) for each window
    hits = window.apply(lambda row: list(sindex.intersection(row)), axis=1)

    # transpose from a list of hits to one entry per hit
    # this implicitly drops any that did not get hits
    tmp = pd.DataFrame(
        {
            # index of points table
            "src_idx": np.repeat(hits.index, hits.apply(len)),
            # ordinal position of line - access via iloc
            "target_i": np.concatenate(hits.values),
        }
    )

    # reset the index on points to get ordinal position, and join to lines and points
    tmp = tmp.join(target_points.reset_index(drop=True), on="target_i").join(
        df.geometry.rename("src_point"), on="src_idx"
    )
    tmp = gp.GeoDataFrame(tmp, geometry="geometry", crs=df.crs)
    tmp["snap_dist"] = tmp.geometry.distance(gp.GeoSeries(tmp.src_point))

    # drop any that are beyond tolerance and sort by distance
    tmp = tmp.loc[tmp.snap_dist <= tolerance].sort_values(by=["src_idx", "snap_dist"])

    # find the nearest line for every point, and count number of lines that are within tolerance
    by_pt = tmp.groupby("src_idx")
    closest = by_pt.first().join(by_pt.size().rename("nearby"))

    # The snapped point is the target point geometry
    snapped = gp.GeoDataFrame(
        closest[target_columns + ["snap_dist", "nearby"]], geometry="geometry"
    )

    # NOTE: this drops any points that didn't get snapped
    return df.drop(columns=["geometry"]).join(snapped).dropna(subset=["geometry"])

