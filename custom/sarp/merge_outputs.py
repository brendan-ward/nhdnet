"""
Merge network analysis outputs across regions, and join back into the pre-processed barriers inventory datasets.
"""

from pathlib import Path
import os
import pandas as pd
import geopandas as gp

from nhdnet.io import (
    serialize_df,
    deserialize_gdf,
    deserialize_df,
    to_shp,
    serialize_gdf,
)

from custom.sarp.constants import REGION_GROUPS, CRS

data_dir = Path("../data/sarp/derived")
out_dir = data_dir / "final_results"
qa_dir = Path("../data/sarp/qa")


for barrier_type in ("dams", "small_barriers"):

    print("Processing {}".format(barrier_type))

    kind = barrier_type[:-1]  # strip off trailing "s"

    merged = None
    for group in REGION_GROUPS:
        print("------- {} -------".format(group))

        df = deserialize_df(
            data_dir / "outputs" / group / barrier_type / "barriers_network.feather"
        )

        # Only keep the barrier type from this analysis
        df = df.loc[df.kind == kind].copy()

        if not len(df):
            print("No {} in group {}".format(barrier_type, group))
            continue

        # Unpack joinID
        df.joinID = df.apply(
            lambda row: row.joinID.replace(row.kind, ""), axis=1
        ).astype("uint")

        if merged is None:
            merged = df
        else:
            merged = merged.append(df, ignore_index=True, sort=False)

    results_df = merged.set_index("joinID", drop=False)

    results_df = results_df[
        [
            "upNetID",
            "UpstreamMiles",
            "NetworkSinuosity",
            "NumSizeClassGained",
            "PctNatFloodplain",
            "downNetID",
            "DownstreamMiles",
            "AbsoluteGainMi",
            "TotalNetworkMiles",
        ]
    ]

    ### Read in pre-processed barriers
    print("Reading pre-processed barriers")
    # drop prioritization columns
    # TODO: move this to prep_*.py step
    barriers_df = (
        deserialize_gdf(data_dir / "inputs" / "{}.feather".format(barrier_type))
        .set_index("joinID")
        .drop(
            columns=[
                "PctNatFloodplain_Score",
                "PctNatFloodplain_Rank",
                "AbsoluteGainMi_Rank",
                "AbsoluteGainMi_Score",
                "NetworkSinuosity_Score",
                "NetworkSinuosity_Rank",
                "NumSizeClass_Score",
                "NumSizeClassesGained_Rank",
                "NumSizeClassGained_Rank",
                "NumSizeClassGained_Score",
                "WatershedCondition_CompositeScore",
                "WatershedCondition_tier",
                "ConnectivityPlusWatershedCondition_CompositeScore",
                "ConnectivityPlusWatershedCondition_tier",
                "Connectivity_CompositeScore",
                "Connectivity_tier",
            ],
            errors="ignore",
        )
        .rename(columns={"batDSNetId": "batDSNetID", "batUSNetId": "batUSNetID"})
    )

    # Set region 8 aside, since the network analysis is done separately
    r8 = barriers_df.loc[barriers_df.HUC2 == "08"].copy()
    r8["NHDplusVersion"] = "Medium"

    # Drop region 8 and columns that come from network analysis
    barriers_df = barriers_df.loc[barriers_df.HUC2 != "08"].drop(
        columns=[c for c in results_df if c in barriers_df.columns]
    )

    ### Join network analysis results to barriers
    print("Joining network analysis results to barriers")

    results_df = barriers_df.join(results_df, how="left").reset_index()
    results_df["NHDplusVersion"] = "High"

    # Run some quick tests to make sure that nothing unexpected happened
    # Have to check before merging in region 8 since those were not snapped in same way
    snapped_no_network = results_df.loc[
        results_df.snapped & results_df.AbsoluteGainMi.isnull()
    ]
    if len(snapped_no_network):
        print(
            "WARNING: {} barriers were snapped but did not get network assigned".format(
                len(snapped_no_network)
            )
        )
        print(
            "These are most likely at the upstream terminals of networks, but should double check"
        )
        to_shp(
            snapped_no_network,
            qa_dir / "{}_snapped_no_network.shp".format(barrier_type),
        )

    # Join region 8 back in
    results_df = results_df.append(
        r8.reset_index(drop=False), sort=False, ignore_index=True
    )

    ### Temporary: Extract join info for networks that cross in to region 8
    # TODO: remove this once region 8 is released
    print("Updating network stats from region 8")
    r8_joins = [
        [514171383, 2219],  # 5 => 8
        [714015373, 2219],  # 7 => 8
        [1114058628, 2109],  # 11 => 8
    ]

    for downNetID, batNetID in r8_joins:
        idx = results_df.downNetID == downNetID
        r8idx = results_df.batDSNetID == batNetID

        if r8idx.max():
            # find the length of this network based on other barriers that already had this downstream ID
            length = results_df.loc[r8idx].iloc[0].DownstreamMiles

            # Add this to the lengths already present
            results_df.loc[idx, "DownstreamMiles"] += length
            results_df.loc[idx, "TotalNetworkMiles"] += length
            results_df.loc[idx, "AbsoluteGainMi"] = results_df.loc[
                idx, ["UpstreamMiles", "DownstreamMiles"]
            ].min(axis=1)

    # TODO: simplify field names for downstream processing

    results_df["HasNetwork"] = ~results_df.AbsoluteGainMi.isnull()

    serialize_gdf(results_df, out_dir / "{}.feather".format(barrier_type), index=False)
    results_df.drop(columns=["geometry"]).to_csv(out_dir / "dams.csv", index=False)

    print("Serializing barriers to shapefile")
    to_shp(results_df, out_dir / "{}.shp".format(barrier_type))

