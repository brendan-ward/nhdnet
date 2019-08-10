"""
Merge barriers output from network analysis for each region into a single shapefile.
"""

from pathlib import Path
import os
import pandas as pd

from nhdnet.io import deserialize_gdf, to_shp

from ..constants import REGION_GROUPS


data_dir = "../data/sarp/derived/outputs"
out_dir = data_dir / "networks"

for barrier_type in ("dams", "small_barriers"):
    print(barrier_type)
    merged = None
    for group in REGION_GROUPS:
        print("------- {} -------".format(group))
        df = deserialize_gdf(
            data_dir / group / barrier_type / "intermediate/barriers.feather"
        )

        if merged is None:
            merged = df
        else:
            merged = merged.append(df, ignore_index=True, sort=False)

    print("Serializing to shapefile")
    to_shp(merged, "{0}/barriers_{1}.shp".format(out_dir, barrier_type))
