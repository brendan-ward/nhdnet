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
    temp["x"] = (temp.geometry.x / tolerance).round().astype("int") * tolerance
    temp["y"] = (temp.geometry.y / tolerance).round().astype("int") * tolerance
    clean = temp.drop_duplicates(subset=["x", "y"], keep="first")
    return df.loc[df.index.isin(clean.index)].copy()

