"""
Extract barriers from data sources, cut by HUC2, and convert to feather format.
"""

import os
import pandas as pd
import geopandas as gp

from nhdnet.io import serialize_gdf, deserialize_gdf, deserialize_sindex
from nhdnet.geometry.lines import snap_to_line

from constants import REGION_GROUPS, REGIONS, CRS, SNAP_TOLERANCE

src_dir = "/Users/bcward/projects/data/sarp"


print("Reading waterfalls")

# Note: this was pre-processed to add in HUC2 codes (via sjoin in pandas)
all_wf = gp.read_file("{}/sarp_falls_huc2.shp".format(src_dir)).to_crs(CRS)[
    ["fall_id", "name", "HUC2", "geometry"]
]
all_wf["joinID"] = all_wf.fall_id.astype("int").astype("str")

snapped = None

for group in REGION_GROUPS:
    print("\n----- {} ------\n".format(group))
    os.chdir("{0}/nhd/region/{1}".format(src_dir, group))

    print("Reading flowlines")
    flowlines = deserialize_gdf("flowline.feather").set_index("lineID", drop=False)
    print("Read {0} flowlines".format(len(flowlines)))

    # print("Calculating spatial index on flowlines")
    # flowlines.sindex
    print("Reading spatial index on flowlines")
    sindex = deserialize_sindex("flowline.sidx")

    ######### Process Waterfalls
    # Extract out waterfalls in this HUC
    wf = all_wf.loc[all_wf.HUC2.isin(REGION_GROUPS[group])].copy()
    print("Selected {0} waterfalls in region".format(len(wf)))

    print("Snapping waterfalls")
    wf = snap_to_line(wf, flowlines, SNAP_TOLERANCE, sindex=sindex)
    print("{} waterfalls were successfully snapped".format(len(wf)))

    if snapped is None:
        snapped = wf
    else:
        snapped = snapped.append(wf, sort=False, ignore_index=True)


print("\n--------------\n")
print("Serializing {0} snapped waterfalls out of {1}".format(len(snapped), len(all_wf)))
serialize_gdf(snapped, "{}/snapped_waterfalls.feather".format(src_dir), index=False)

