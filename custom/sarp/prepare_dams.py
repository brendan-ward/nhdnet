import os
import pandas as pd
import geopandas as gp

from nhdnet.io import serialize_gdf, deserialize_gdf, deserialize_sindex
from nhdnet.geometry.lines import snap_to_line

from constants import REGION_GROUPS, REGIONS, CRS, SNAP_TOLERANCE


src_dir = "/Users/bcward/projects/data/sarp"

snapped = None

for group in REGION_GROUPS:
    print("\n----- {} ------\n".format(group))
    os.chdir("{0}/nhd/region/{1}".format(src_dir, group))

    print("Reading flowlines")
    flowlines = deserialize_gdf("flowline.feather").set_index("lineID", drop=False)
    print("Read {0} flowlines".format(len(flowlines)))

    print("Reading spatial index on flowlines")
    sindex = deserialize_sindex("flowline.sidx")

    ######### Process Dams
    for HUC2 in REGION_GROUPS[group]:
        print("Reading dams in {}".format(HUC2))

        # Read in manually snapped dams
        dams = gp.read_file("{0}/snapped_dams/dams{1}qa.shp".format(src_dir, HUC2))
        dams["HUC2"] = HUC2
        dams["joinID"] = dams.AnalysisID

        # Note: this includes some dams that shouldn't snap (>200 m) but were accidentally included in those sent out for
        # manual snapping.  Remove them.
        dams = dams.loc[~(dams.NHDPlusID.isnull() | (dams.NHDPlusID == 0))]

        # manually snapped dams have had some dams removed, so expect this not to match perfectly with QA
        # remove any that are marked with a SNAP2018 of 5, 6, 7 (manually determined NOT on network)
        # remove any dams that are marked with a SNAP2018 of 8 (dam was removed for conservation)
        dams = dams.loc[~dams.SNAP2018.isin([5, 6, 7, 8])]

        print("Selected {0} valid dams within region".format(len(dams)))

        dams = dams[["joinID", "AnalysisID", "HUC2", "geometry"]].copy()

        print("Snapping dams")
        sidx = "flowline.sidx"
        dams = snap_to_line(dams, flowlines, SNAP_TOLERANCE, sindex=sindex)
        print("{} dams were successfully snapped".format(len(dams)))

        if snapped is None:
            snapped = dams

        else:
            snapped = snapped.append(dams, sort=False, ignore_index=True)

print("Serializing {0} snapped dams".format(len(snapped)))
serialize_gdf(snapped, "{}/snapped_dams.feather".format(src_dir), index=False)

