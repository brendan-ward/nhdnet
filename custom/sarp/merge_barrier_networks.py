import os
import pandas as pd

from constants import REGION_GROUPS


src_dir = "/Users/bcward/projects/data/sarp"

out_dir = "{}/networks".format(src_dir)

for barrier_type in ("dams", "small_barriers"):
    print(barrier_type)
    merged = None
    for group in REGION_GROUPS:
        print("------- {} -------".format(group))
        df = pd.read_csv(
            "{0}/nhd/region/{1}/barriers_network_{2}.csv".format(
                src_dir, group, barrier_type
            )
        )

        if merged is None:
            merged = df
        else:
            merged = merged.append(df, ignore_index=True, sort=False)

    merged.to_csv(
        "{0}/barriers_network_{1}.csv".format(out_dir, barrier_type), index=False
    )

