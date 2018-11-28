# from pandas import read_feather # The pandas version has a feather versioning issue
import os
import json
from feather import read_dataframe
from geopandas import GeoDataFrame
from shapely.wkb import loads


def serialize_df(df, path, index=True):
    """Serializes a pandas DataFrame to a feather file on disk.

    If the data frame has a non-default index, that is added back as a column before writing out.
    
    Parameters
    ----------
    df : pandas.DataFrame
    path : str
        path to feather file to write
    index : bool
        if False, will drop the index
    """
    df = df.copy()

    # If df has a non-default index, convert it back to a column
    # if the associated column is not found
    if df.index.name or not index:
        df.reset_index(inplace=True, drop=not index or df.index.name in df.columns)

    df.to_feather(path)


def serialize_gdf(df, path, index=True):
    """Serializes a geopandas GeoDataFrame to a feather file on disk.

    If the data frame has a non-default index, that is added back as a column before writing out.

    Internally, the geometry data are converted to WKB format.

    This also creates a .crs file with CRS information for this dataset
    
    Parameters
    ----------
    df : geopandas.GeoDataFrame
    path : str
        path to feather file to write
    index : bool
        if False, will drop the index
    """

    # TODO: save an attribute indicating the index col?

    # write the crs to an associated file
    if df.crs:
        with open("{}.crs".format(path), "w") as crsfile:
            crs = df.crs
            if isinstance(crs, str):
                crs = {"proj4": crs}
            crsfile.write(json.dumps(crs))

    df = df.copy()

    # If df has a non-default index, convert it back to a column
    # if the associated column is not found
    if df.index.name or not index:
        df.reset_index(inplace=True, drop=not index or df.index.name in df.columns)

    df["wkb"] = df.geometry.apply(lambda g: g.to_wkb())
    df = df.drop(columns=["geometry"])
    df.to_feather(path)


def deserialize_df(path):
    """Deserialize a pandas.DataFrame stored in a feather file.

    Note: no index is set on this after deserialization, that is the responsibility of the caller.
    
    Parameters
    ----------
    path : str
        path to feather file to read
    
    Returns
    -------
    pandas.DataFrame
    """

    return read_dataframe(path)


def deserialize_gdf(path):
    """Deserialize a geopandas.GeoDataFrame stored in a feather file.

    This converts the internal WKB representation back into geometry.

    If the corresponding .crs file is found, it is used to set the CRS of
    the GeoDataFrame.

    Note: no index is set on this after deserialization, that is the responsibility of the caller.
    
    Parameters
    ----------
    path : str
        path to feather file to read
    
    Returns
    -------
    geopandas.GeoDataFrame
    """

    crs = None
    crsfilename = "{}.crs".format(path)
    if os.path.exists(crsfilename):
        crs = json.loads(open(crsfilename).read())
        if "proj4" in crs:
            crs = crs["proj4"]

    df = read_dataframe(path)
    df["geometry"] = df.wkb.apply(lambda wkb: loads(wkb))
    return GeoDataFrame(df.drop(columns=["wkb"]), geometry="geometry", crs=crs)
