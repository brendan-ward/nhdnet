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

barrier_type = "dams"
data_dir = Path("../data/sarp/derived")
results_dir = data_dir / "final_results"

metrics = [
    "AbsoluteGainMi",
    "UpstreamMiles",
    "DownstreamMiles",
    "TotalNetworkMiles",
    "NumSizeClassGained",
    "PctNatFloodplain",
    "NetworkSinuosity",
]

results_df = (
    deserialize_gdf(results_dir / "{}.feather".format(barrier_type))
    .dropna(subset=["AbsoluteGainMi"])
    .set_index("joinID")
)
# Only compare those that previously had networks
prev = (
    deserialize_gdf(data_dir / "inputs" / "{}.feather".format(barrier_type))
    .dropna(subset=["AbsoluteGainMi"])
    .set_index("joinID")[metrics]
)

qa = results_df[metrics + ["HasNetwork"]].join(prev, rsuffix="_prev", how="inner")

# Are there any that had networks that now don't?
qa["HasNetwork_prev"] = ~qa.AbsoluteGainMi_prev.isnull()
diff_idx = qa["HasNetwork"] != qa["HasNetwork_prev"]
print(
    "Difference in HasNetwork: {} ({} had networks previously)".format(
        len(qa.loc[diff_idx]), len(qa.loc[diff_idx & qa.HasNetwork_prev])
    )
)

# Some differences in network metrics are expected as dams are added and removed, but probably not big differences for most
metric = "AbsoluteGainMi"
diff = qa[metric] - qa["{}_prev".format(metric)]
diff = diff[diff.abs() >= 0.1].sort_values()
print("Stats of difference between AbsoluteGainMiles")
print(diff.describe())


for metric in metrics:
    print("Checking {}".format(metric))
    diff = qa[metric] - qa["{}_prev".format(metric)]

    # only look at those that are a good bit different
    diff = diff[diff.abs() >= 0.1]

    print("{} records are >= 0.1 different for {}".format(len(diff), metric))
