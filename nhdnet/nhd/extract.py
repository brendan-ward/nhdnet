"""Extract NHD data to simpler data formats for later processing

Run this first!

1. Read NHDFlowline and convert to 2D lines
2. Join to VAA and bring in select attributes
2. Project to USGS CONUS Albers (transient geom)
3. Calculate sinuosity and length
4. Write to shapefile
5. Write to CSV

Note: NHDPlusIDs are converted to uint64 for internal processing. 
These need to be converted back to float64 for use in shapefiles and such

TODO: add other attributes to keep throughout, including size info for plotting

If a HUC4 raises an invalid geometry error when trying to read it, use ogr2ogr to convert it first:
ogr2ogr -f "ESRI Shapefile" NHDFlowline.shp  NHDPLUS_H_0601_HU4_GDB.gdb NHDFlowline

"""

import os
import geopandas as gp
import pandas as pd
from shapely.geometry import MultiLineString
from nhdnet.geometry.lines import to2D, calculate_sinuosity


FLOWLINE_COLS = ["NHDPlusID", "FlowDir", "FType", "GNIS_ID", "GNIS_Name", "geometry"]

# TODO: add elevation gradient info
VAA_COLS = ["NHDPlusID", "StreamOrde", "StreamCalc", "TotDASqKm"]


def extract_flowlines(gdb_path, target_crs, extra_flowline_cols=[]):
    """
    Extracts data from NHDPlusHR data product.
    Extract flowlines, join to VAA table, and filter out any loops and coastlines.
    Extract joins between flowlines, and filter out any loops and coastlines.
    
    Parameters
    ----------
    gdb_path : str
        path to the NHD HUC4 Geodatabase
    target_crs: GeoPandas CRS object
        target CRS to project NHD to for analysis, like length calculations.  
        Must be a planar projection.
    extra_cols: list
        List of extra field names to extract from NHDFlowline layer

    Returns
    -------
    type of geopandas.GeoDataFrame
        (flowlines, joins)
    """

    # Read in data and convert to data frame (no need for geometry)
    print("Reading flowlines")

    flowline_cols = FLOWLINE_COLS + extra_flowline_cols

    df = gp.read_file(gdb_path, layer="NHDFlowline")[flowline_cols]

    # Set our internal master IDs to the original index of the file we start from
    # Assume that we can always fit into a uint32, which is ~400 million records
    # and probably bigger than anything we could ever read in
    df["lineID"] = df.index.values.astype("uint32") + 1
    # Index on NHDPlusID for easy joins to other NHD data
    df.NHDPlusID = df.NHDPlusID.astype("uint64")
    df = df.set_index(["NHDPlusID"], drop=False)

    print("Read {:,} flowlines".format(len(df)))

    # Read in VAA and convert to data frame
    # NOTE: not all records in Flowlines have corresponding records in VAA
    # we drop those that do not since we need these fields.
    print("Reading VAA table and joining...")
    vaa_df = gp.read_file(gdb_path, layer="NHDPlusFlowlineVAA")[VAA_COLS]
    vaa_df.NHDPlusID = vaa_df.NHDPlusID.astype("uint64")
    vaa_df = vaa_df.set_index(["NHDPlusID"])
    df = df.join(vaa_df, how="inner")
    print("{:,} features after join to VAA".format(len(df)))

    # Rename fields
    df = df.rename(columns={"StreamOrde": "streamorder"})

    # Filter out loops (query came from Kat) and other segments we don't want.
    # 566 is coastlines type.
    print("Filtering out loops and coastlines")
    coastline_idx = df.loc[(df.FType == 566)].index
    removed_idx = df.loc[
        (df.streamorder != df.StreamCalc) | (df.FlowDir.isnull()) | (df.FType == 566)
    ].index
    df = df.loc[~df.index.isin(removed_idx)].copy()
    print("{:,} features after removing loops and coastlines".format(len(df)))

    # Calculate size classes
    print("Calculating size class")
    drainage = df.TotDASqKm
    df.loc[drainage < 10, "sizeclass"] = "1a"
    df.loc[(drainage >= 10) & (drainage < 100), "sizeclass"] = "1b"
    df.loc[(drainage >= 100) & (drainage < 518), "sizeclass"] = "2"
    df.loc[(drainage >= 518) & (drainage < 2590), "sizeclass"] = "3a"
    df.loc[(drainage >= 2590) & (drainage < 10000), "sizeclass"] = "3b"
    df.loc[(drainage >= 10000) & (drainage < 25000), "sizeclass"] = "4"
    df.loc[drainage >= 25000, "sizeclass"] = "5"

    # convert to LineString from MultiLineString
    if df.iloc[0].geometry.geom_type == "MultiLineString":
        print("Converting MultiLineString => LineString")
        df.geometry = df.geometry.apply(
            lambda g: g[0] if isinstance(g, MultiLineString) else g
        )

    # Convert incoming data from XYZM to XY
    print("Converting geometry to 2D")
    df.geometry = df.geometry.apply(to2D)

    print("projecting to target projection")
    df = df.to_crs(target_crs)

    # Calculate length and sinuosity
    print("Calculating length and sinuosity")
    df["length"] = df.geometry.length.astype("float32")
    df["sinuosity"] = df.geometry.apply(calculate_sinuosity).astype("float32")

    # Drop columns we don't need any more for faster I/O
    df = df.drop(columns=["FlowDir", "StreamCalc"])

    # Simplify data types for smaller files and faster IO
    df.FType = df.FType.astype("uint16")
    df.streamorder = df.streamorder.astype("uint8")

    ############# Connections between segments ###################
    print("Reading segment connections")
    join_df = gp.read_file(gdb_path, layer="NHDPlusFlow")[
        ["FromNHDPID", "ToNHDPID"]
    ].rename(columns={"FromNHDPID": "upstream", "ToNHDPID": "downstream"})
    join_df.upstream = join_df.upstream.astype("uint64")
    join_df.downstream = join_df.downstream.astype("uint64")

    # remove any joins that have coastlines as upstream
    # these are themselves coastline segments
    join_df = join_df.loc[~join_df.upstream.isin(coastline_idx)].copy()

    # set the downstream to 0 for any that join coastlines
    # this will enable us to mark these as downstream terminals in
    # the network analysis later
    join_df.loc[join_df.downstream.isin(coastline_idx), "downstream"] = 0

    # drop any duplicates (above operation sets some joins to upstream and downstream of 0)
    join_df = join_df.drop_duplicates()

    # remove any other joins to or from segments we removed above
    join_df = join_df.loc[
        ~(join_df.upstream.isin(removed_idx) | join_df.downstream.isin(removed_idx))
    ]

    # update joins with our ids
    ids = df[["lineID"]]
    join_df = (
        join_df.join(ids.rename(columns={"lineID": "upstream_id"}), on="upstream")
        .join(ids.rename(columns={"lineID": "downstream_id"}), on="downstream")
        .fillna(0)
    )

    for col in ("upstream", "downstream"):
        join_df[col] = join_df[col].astype("uint64")

    for col in ("upstream_id", "downstream_id"):
        join_df[col] = join_df[col].astype("uint32")

    # set join types to make it easier to track
    join_df["type"] = "internal"  # set default
    join_df.loc[join_df.upstream == 0, "type"] = "origin"
    join_df.loc[join_df.downstream == 0, "type"] = "terminal"
    join_df.loc[(join_df.upstream != 0) & (join_df.upstream_id == 0), "type"] = "huc_in"

    # Note: this doesn't seem to be working, because outflowing rivers are coded as 0 by NHD
    # join_df.loc[(join_df.downstream != 0) & (join_df.downstream_id == 0), 'type'] = 'huc_out'

    return df, join_df

