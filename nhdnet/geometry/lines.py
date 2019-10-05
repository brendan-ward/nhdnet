import pandas as pd
import geopandas as gp
import numpy as np
from shapely.geometry import Point, LineString, MultiLineString


def to2D(geometry):
    """Flatten a 3D line to 2D.
    
    Parameters
    ----------
    geometry : LineString
        Input 3D geometry
    
    Returns
    -------
    LineString
        Output 2D geometry
    """

    return LineString(np.column_stack(geometry.xy))

    # if geometry.type == "MultiLineString":
    #     return MultiLineString([LineString(c[:2] for c in g.coords) for g in geometry])
    # return LineString(c[:2] for c in geometry.coords)


def calculate_sinuosity(geometry):
    """Calculate sinuosity of the line.

    This is the length of the line divided by the distance between the endpoints of the line.
    By definition, it is always >=1.
    
    Parameters
    ----------
    geometry : LineString
    
    Returns
    -------
    float
        sinuosity value
    """

    # By definition, sinuosity should not be less than 1
    line = geometry
    straight_line_distance = Point(line.coords[0]).distance(Point(line.coords[-1]))
    if straight_line_distance > 0:
        return max(line.length / straight_line_distance, 1)

    return 1  # if there is no straight line distance, there is no sinuosity


def snap_to_line(points, lines, tolerance=100, sindex=None):
    """
    Attempt to snap a line to the nearest line, within tolerance distance.

    Lines must be in a planar (not geographic) projection and points 
    must be in the same projection.

    Parameters
    ----------
    points : GeoPandas.DataFrame
        points to snap
    lines : GeoPandas.DataFrame
        lines to snap against 
    tolerance : int, optional (default: 100)
        maximum distance between line and point that can still be snapped

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

    # get list of columns to copy from flowlines
    line_columns = lines.columns[lines.columns != "geometry"].to_list()

    # generate spatial index if it is missing
    if sindex is None:
        sindex = lines.sindex
        # Note: the spatial index is ALWAYS based on the integer index of the
        # geometries and NOT their index

    # generate a window around each point
    window = points.bounds + [-tolerance, -tolerance, tolerance, tolerance]
    # get a list of the line ordinal line indexes (integer index, not actual index) for each window
    # points['line_hits'] =
    hits = window.apply(lambda row: list(sindex.intersection(row)), axis=1)

    # transpose from a list of hits to one entry per hit
    # this implicitly drops any that did not get hits
    tmp = pd.DataFrame(
        {
            # index of points table
            "pt_idx": np.repeat(hits.index, hits.apply(len)),
            # ordinal position of line - access via iloc
            "line_i": np.concatenate(hits.values),
        }
    )

    # reset the index on lines to get ordinal position, and join to lines and points
    tmp = tmp.join(lines.reset_index(drop=True), on="line_i").join(
        points.geometry.rename("point"), on="pt_idx"
    )
    tmp = gp.GeoDataFrame(tmp, geometry="geometry", crs=points.crs)
    tmp["snap_dist"] = tmp.geometry.distance(gp.GeoSeries(tmp.point))

    # drop any that are beyond tolerance and sort by distance
    tmp = tmp.loc[tmp.snap_dist <= tolerance].sort_values(by=["pt_idx", "snap_dist"])

    # find the nearest line for every point, and count number of lines that are within tolerance
    by_pt = tmp.groupby("pt_idx")
    closest = gp.GeoDataFrame(
        by_pt.first().join(by_pt.size().rename("nearby")), geometry="geometry"
    )

    # now snap to the line
    # project() calculates the distance on the line closest to the point
    # interpolate() generates the point actually on the line at that point
    snapped_pt = closest.interpolate(
        closest.geometry.project(gp.GeoSeries(closest.point))
    )
    snapped = gp.GeoDataFrame(
        closest[line_columns + ["snap_dist", "nearby"]], geometry=snapped_pt
    )

    # NOTE: this drops any points that didn't get snapped
    return points.drop(columns=["geometry"]).join(snapped).dropna(subset=["geometry"])


def snap_to_line_old(points, lines, tolerance=100, sindex=None):
    """
    Attempt to snap a line to the nearest line, within tolerance distance.

    Lines must be in a planar (not geographic) projection and points 
    must be in the same projection.

    Parameters
    ----------
    points : GeoPandas.DataFrame
        points to snap
    lines : GeoPandas.DataFrame
        lines to snap against 
    tolerance : int, optional (default: 100)
        maximum distance between line and point that can still be snapped

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

    line_columns = list(set(lines.columns).difference({"geometry"}))
    columns = ["geometry", "snap_dist", "nearby"] + line_columns

    def snap(point):
        # point = record.geometry
        x, y = point.coords[0][:2]

        # Search window
        window = (x - tolerance, y - tolerance, x + tolerance, y + tolerance)

        # find nearby features
        hits = lines.iloc[list(sindex.intersection(window))].copy()

        # calculate distance to point and
        hits["dist"] = hits.distance(point)
        within_tolerance = hits[hits.dist <= tolerance]

        if len(within_tolerance):
            # find nearest line segment that is within tolerance
            closest = within_tolerance.nsmallest(1, columns=["dist"]).iloc[0]
            line = closest.geometry

            dist = closest.dist

            snapped = line.interpolate(line.project(point))

            values = [snapped, dist, len(within_tolerance)]

            # Copy attributes from line to point
            values.extend([closest[c] for c in line_columns])

            return gp.GeoSeries(values, index=columns)

        # create empty record
        return pd.Series([None] * len(columns), index=columns)

    if sindex is None:
        sindex = lines.sindex
        # Note: the spatial index is ALWAYS based on the integer index of the
        # geometries and NOT their index

    snapped = gp.GeoDataFrame(points.geometry.apply(snap), crs=points.crs)
    points = points.drop(columns=["geometry"]).join(snapped)
    return points.loc[~points.geometry.isnull()].copy()


def cut_line_at_point(line, point):
    """
    Cut line at a point on the line.
    modified from: https://shapely.readthedocs.io/en/stable/manual.html#splitting

    Parameters
    ----------
    line : shapely.LineString
    point : shapely.Point
    
    Returns
    -------
    list of LineStrings
    """

    distance = line.project(point)
    if distance <= 0.0 or distance >= line.length:
        return [LineString(line)]

    coords = list(line.coords)
    for i, p in enumerate(coords):
        pd = line.project(Point(p))
        if pd == distance:
            return [LineString(coords[: i + 1]), LineString(coords[i:])]
        if pd > distance:
            cp = line.interpolate(distance)
            return [
                LineString(coords[:i] + [(cp.x, cp.y)]),
                LineString([(cp.x, cp.y)] + coords[i:]),
            ]


def cut_line_at_points(line, points):
    """
    Cut a line geometry by multiple points.
    
    Parameters
    ----------
    line : shapely.LineString
    points : iterable of shapely.Point objects.  
        Must be ordered from the start of the line to the end.
       
    
    Returns
    -------
    list of shapely.LineString containing new segments
    """

    segments = []
    remainder = line

    for point in points:
        segment, remainder = cut_line_at_point(remainder, point)
        segments.append(segment)

    segments.append(remainder)

    return segments
