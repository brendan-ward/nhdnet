# from pandas import read_feather # The pandas version has a feather versioning issue
import os
import json
import numpy as np
import warnings
from pandas import DataFrame
from feather import read_dataframe
from geofeather import to_geofeather, from_geofeather
from geopandas import GeoDataFrame
from geopandas.io.file import infer_schema
from shapely.wkb import loads
from shapely.geometry import mapping
import fiona
from rtree.index import Index, Property


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

    warnings.warn(
        "serialize_gdf() is deprecated.  Use geofeather::to_geofeather() instead",
        DeprecationWarning,
    )

    to_geofeather(df, path)


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


def deserialize_dfs(paths, src=None):
    """Deserialize multiple pandas.DataFrames stored in feather files.
    
    Parameters
    ----------
    paths : str
        iterable of paths to feather files
    src : list (optional)
        if present, must be same length as paths, and will be used to set a 'src'
        column in the output data frame
    
    Returns
    -------
    pandas.DataFrame
    """

    merged = None
    for index, path in enumerate(paths):
        df = deserialize_df(path)

        if src is not None:
            df["src"] = src[index]

        if merged is None:
            merged = df

        else:
            merged = merged.append(df, ignore_index=True, sort=False)

    return merged


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

    warnings.warn(
        "deserialize_gdf() is deprecated.  Use geofeather::to_geofeather() instead",
        DeprecationWarning,
    )

    return from_geofeather(path)


def deserialize_gdfs(paths, src=None):
    """Deserialize multiple geopandas.GeoDataFrames stored in feather files.
    
    Parameters
    ----------
    paths : str
        iterable of paths to feather files
    src : list (optional)
        if present, must be same length as paths, and will be used to set a 'src'
        column in the output data frame
    

    Returns
    -------
    geopandas.GeoDataFrame
    """

    merged = None
    for index, path in enumerate(paths):
        df = deserialize_gdf(path)

        if src is not None:
            df["src"] = src[index]

        if merged is None:
            merged = df

        else:
            merged = merged.append(df, ignore_index=True, sort=False)

    return merged


def to_shp(df, path):
    geom_col = df._geometry_column_name

    # Drop any records with missing geometries
    df = df.loc[~df[geom_col].isnull()].copy()

    # Convert data types to those supported by shapefile
    for c in [c for c, t in df.dtypes.items() if t == "uint64"]:
        df[c] = df[c].astype("float64")

    df.to_file(path)

    ### Original implementation, now slower than geopandas to_file()
    # geometry = df[geom_col].apply(mapping)
    # # fill missing data with None and convert to dict
    # props = df.drop(columns=[df._geometry_column_name])
    # props.replace({c: {np.nan: None} for c in prop_cols}, inplace=True)
    # props = props.apply(lambda row: row.to_dict(), axis=1)
    # # Convert features to JSON
    # features = DataFrame({"geometry": geometry, "properties": props})
    # features["type"] = "Feature"
    # features = features.apply(lambda row: row.to_dict(), axis=1)
    # schema = infer_schema(df)
    # with fiona.Env():
    #     with fiona.open(
    #         path, "w", driver="ESRI Shapefile", crs=df.crs, schema=schema
    #     ) as writer:
    #         writer.writerecords(features)


def serialize_sindex(df, path):
    """Serialize the bounding coordinates necessary to recreate a spatial index

    Parameters
    ----------
    df : geopandas.GeoDataFrame
        contains geometries and optional index, all other columns are ignored
    path : str
        path to write spatial index
    """

    df = df[["geometry"]].join(df.geometry.bounds.astype("float32"))
    df = df.reset_index(drop=not df.index.name)
    serialize_df(df.drop(columns=["geometry"]), path)


def deserialize_sindex(path):
    """Converts serialized bounding coordinates into an rtree spatial index

    Parameters
    ----------
    path : str
        path to spatial index

    Returns
    -------
    rtree.index.Index instance
    """
    df = deserialize_df(path)
    extra_cols = df.columns.drop(["minx", "miny", "maxx", "maxy"])
    if len(extra_cols):
        index_col = extra_cols[0]
    else:
        index_col = "i"
    df["i"] = df.index.values
    df["b"] = df[["minx", "miny", "maxx", "maxy"]].values.tolist()
    stream = df[["i", "b", index_col]].values.tolist()
    # Note: do not pass in as stream parameter.  Docs impliy that should work.  It doesn't!
    return Index(stream, properties=Property(leaf_capacity=1000))
