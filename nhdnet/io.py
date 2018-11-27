# from pandas import read_feather # The pandas version has a feather versioning issue
import os
import json
from feather import read_dataframe
from geopandas import GeoDataFrame
from shapely.wkb import loads


def serialize(df, path):
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
    if df.index.name:
        df.reset_index(inplace=True, drop=df.index.name in df.columns)

    df["wkb"] = df.geometry.apply(lambda g: g.to_wkb())
    df = df.drop(columns=["geometry"])
    df.to_feather(path)


def deserialize(path):
    crs = None
    crsfilename = "{}.crs".format(path)
    if os.path.exists(crsfilename):
        crs = json.loads(open(crsfilename).read())
        if "proj4" in crs:
            crs = crs["proj4"]

    df = read_dataframe(path)
    df["geometry"] = df.wkb.apply(lambda wkb: loads(wkb))
    return GeoDataFrame(df.drop(columns=["wkb"]), geometry="geometry", crs=crs)
