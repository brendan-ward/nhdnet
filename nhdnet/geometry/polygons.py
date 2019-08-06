import numpy as np
from shapely.geometry import MultiPolygon, Polygon, LinearRing


def to2D(geometry):
    """Flatten a 3D polygon to 2D.
    
    Parameters
    ----------
    geometry : Polygon
        Input 3D geometry
    
    Returns
    -------
    Polygon
        Output 2D geometry
    """

    if geometry.type == "MultiPolygon":
        return MultiPolygon([to2D(p) for p in geometry])

    exterior = LinearRing(np.column_stack(geometry.exterior.xy))
    interiors = [
        LinearRing(np.column_stack(interior.xy)) for interior in geometry.interiors
    ]
    return Polygon(exterior, interiors)
