"""
Floodplain stats were generated in ArcGIS by
1. developing a floodplain mask from existing data sources and 90m around all flowlines
2. developing a binary map of natural landcover / not natural landcover
3. clipping landcover by floodplain mask
4. running zonal stats to calculate the area in natural landcover and not natural landcover in the floodplain mask

Note: some catchments have no floodplain, and some have floodplains but no NHDPlusID (outside HUC4s we processed).  These are filtered out.
"""


import pandas as pd
import geopandas as gp

from nhdnet.io import serialize_df
from constants import REGIONS, REGION_GROUPS

src_dir = "/Users/bcward/projects/data/sarp"
gdb = "{}/Catchment_floodplain_stats.gdb".format(src_dir)

merged = None
for group in REGION_GROUPS:
    for HUC2 in REGION_GROUPS[group]:
        print("Process floodplain stats for {}".format(HUC2))
        lyr_huc2 = "0{}".format(HUC2) if HUC2 in ("10", "11", "12", "13") else HUC2
        df = gp.read_file(gdb, layer="FloodplainStats_{}".format(lyr_huc2))[
            ["NHDPlusID", "VALUE_0", "VALUE_1"]
        ]
        # Drop any entries that do not have a floodplain
        df = df.dropna(subset=["NHDPlusID", "VALUE_0"])
        df.NHDPlusID = df.NHDPlusID.astype("uint64")
        df["HUC2"] = HUC2

        # calculate total floodplain area and amount in natural landcover
        # Note: VALUE_0 and VALUE_1 are in sq meters
        df["floodplain_km2"] = (df["VALUE_0"] + df["VALUE_1"]) * 1e-6
        df["nat_floodplain_km2"] = df["VALUE_1"] * 1e-6

        df = df[["NHDPlusID", "HUC2", "floodplain_km2", "nat_floodplain_km2"]]

        if merged is None:
            merged = df
        else:
            merged = merged.append(df, ignore_index=True, sort=False)

serialize_df(merged, "{0}/floodplain_stats.feather".format(src_dir), index=False)
