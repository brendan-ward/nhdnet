import os
import pandas as pd

from nhdnet.io import deserialize_gdf, to_shp

from constants import REGION_GROUPS


src_dir = "/Users/bcward/projects/data/sarp"

out_dir = "{}/networks".format(src_dir)

for barrier_type in ("dams", "small_barriers"):
    print(barrier_type)
    merged = None
    for group in REGION_GROUPS:
        print("------- {} -------".format(group))
        df = deserialize_gdf(
            "{0}/nhd/region/{1}/barriers_{2}.feather".format(
                src_dir, group, barrier_type
            )
        )

        if merged is None:
            merged = df
        else:
            merged = merged.append(df, ignore_index=True, sort=False)

    print("Serializing to shapefile")
    to_shp(merged, "{0}/barriers_{1}.shp".format(out_dir, barrier_type))
