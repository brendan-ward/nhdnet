import geopandas as gp
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
