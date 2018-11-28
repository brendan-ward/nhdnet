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

    # if geometry.type == "MultiLineString":
    #     total_length = 0
    #     results = []
    #     for line in geometry:
    #         length = line.length
    #         total_length += length
    #         straight_line_distance = Point(line.coords[0]).distance(
    #             Point(line.coords[-1])
    #         )

    #         if straight_line_distance > 0:
    #             sinuosity = length / straight_line_distance
    #             results.append((length, sinuosity))

    #     if total_length == 0:
    #         return 1

    #     # return weighted sum
    #     return max(
    #         sum([(length / total_length) * sinuosity for length, sinuosity in results]),
    #         1,
    #     )

    # By definition, sinuosity should not be less than 1
    line = geometry
    straight_line_distance = Point(line.coords[0]).distance(Point(line.coords[-1]))
    if straight_line_distance > 0:
        return max(line.length / straight_line_distance, 1)

    return 1  # if there is no straight line distance, there is no sinuosity


def snap_to_line(lines, tolerance=100, prefer_endpoint=False):
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
    prefer_endpoint : bool, optional (default False)
        if True, will try to match to the nearest endpoint on the nearest line
        provided that the distance to that endpoint is less than tolerance

    Returns
    -------
    geopandas.GeoDataFrame
        output data frame containing: 
        * geometry: snapped geometry
        * snap_dist: distance between original point and snapped location
        * nearby: number of nearby lines within tolerance
        * is_endpoint: True if successfully snapped to endpoint
        * any columns joined from lines
    """

    line_columns = list(set(lines.columns).difference({"geometry"}))

    def snap(record):
        point = record.geometry
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
            snapped = None
            is_endpoint = False
            if prefer_endpoint:
                # snap to the nearest endpoint if it is within tolerance
                endpoints = [
                    (pt, point.distance(pt))
                    for pt in (Point(line.coords[0]), Point(line.coords[-1]))
                    if point.distance(pt) < tolerance
                ]
                endpoints = sorted(endpoints, key=lambda x: x[1])
                if endpoints:
                    snapped, dist = endpoints[0]
                    is_endpoint = True

            if snapped is None:
                snapped = line.interpolate(line.project(point))

            columns = ["geometry", "snap_dist", "nearby", "is_endpoint"]
            values = [snapped, dist, len(within_tolerance), is_endpoint]

            # Copy attributes from line to point
            columns.extend(line_columns)
            values.extend([closest[c] for c in line_columns])

            return gp.GeoSeries(values, index=columns)

    print("creating spatial index on lines")
    sindex = lines.sindex
    # Note: the spatial index is ALWAYS based on the integer index of the
    # geometries and NOT their index

    # we are calling this as a curried function, so we return the func
    # for the apply()
    return snap


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
