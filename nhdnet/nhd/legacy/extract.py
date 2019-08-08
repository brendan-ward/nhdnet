"""Process Medium resolution NHD data.

Reference: https://prd-wret.s3-us-west-2.amazonaws.com/assets/palladium/production/s3fs-public/atoms/files/NHDv2.2.1_poster_081216.pdf

"""

import geopandas as gp


from nhdnet.nhd.extract import FLOWLINE_COLS, VAA_COLS


def extract_flowlines_mr(gdb_path, target_crs):
    """
    Extracts data from NHDPlus Medium Resolution data product.
    Extract flowlines, join to VAA table, and filter out any loops and coastlines.
    Extract joins between flowlines, and filter out any loops and coastlines.
    
    Parameters
    ----------
    gdb_path : str
        path to the NHD HUC4 Geodatabase
    target_crs: GeoPandas CRS object
        target CRS to project NHD to for analysis.  Must be a planar projection.

    Returns
    -------
    type of geopandas.GeoDataFrame
        (flowlines, joins)
    """

    # Read in data and convert to data frame (no need for geometry)
    print("Reading flowlines")

    # WARNING: this NHDPlusID is not equivalent to that used by high resolution
    df = gp.read_file(gdb_path, layer="NHDFlowline").rename(
        columns={"Permanent_Identifier": "NHDPlusID"}
    )

    df = df[FLOWLINE_COLS]
    # Set our internal master IDs to the original index of the file we start from
    # Assume that we can always fit into a uint32, which is ~400 million records
    # and probably bigger than anything we could ever read in
    df["lineID"] = df.index.values.astype("uint32") + 1
    df = df.set_index(["NHDPlusID"], drop=False)

    print("Read {} flowlines".format(len(df)))

    # Read in VAA and convert to data frame
    # NOTE: not all records in Flowlines have corresponding records in VAA
    print("Reading VAA table and joining...")
    vaa_df = gp.read_file(gdb_path, layer="NHDFlowlineVAA").rename(
        columns={"Permanent_Identifier": "NHDPlusID", "StreamOrder": "StreamOrde"}
    )[VAA_COLS]
    vaa_df = vaa_df.set_index(["NHDPlusID"])
    df = df.join(vaa_df, how="inner")
    print("{} features after join to VAA".format(len(df)))

    # Filter out loops (query came from Kat) and other segments we don't want.
    # 566 is coastlines type.
    print("Filtering out loops and coastlines")
    # (df.StreamOrde != df.StreamCalc)
    removed = df.loc[(df.FlowDir.isnull()) | (df.FType == 566)]
    df = df.loc[~df.index.isin(removed.index)].copy()
    print("{} features after removing loops and coastlines".format(len(df)))

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
    df = df.drop(columns=["FlowDir", "TotDASqKm", "StreamCalc"])

    ############# Connections between segments ###################
    print("Reading segment connections")
    join_df = gp.read_file(gdb_path, layer="NHDFlow").rename(
        columns={
            "From_Permanent_Identifier": "upstream",
            "To_Permanent_Identifier": "downstream",
        }
    )[["upstream", "downstream"]]

    # remove any joins to or from segments we removed above
    join_df = join_df.loc[
        ~(join_df.upstream.isin(removed.index) | join_df.downstream.isin(removed.index))
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

    return df, join_df
