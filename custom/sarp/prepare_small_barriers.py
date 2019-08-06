"""
Extract small barriers from original data source, process for use in network analysis, and convert to feather format.
1. Cleanup data values (as needed)
2. Filter out barriers not to be included in analysis (based on Potential_Project and Snap2018)
3. Remove duplicate barriers
4. Snap to networks by HUC2 and merge into single data frame
"""

from pathlib import Path
from time import time
import pandas as pd
import geopandas as gp

from nhdnet.io import serialize_gdf, deserialize_gdf, deserialize_sindex, to_shp
from nhdnet.geometry.points import remove_duplicates
from nhdnet.geometry.lines import snap_to_line

from constants import (
    REGION_GROUPS,
    REGIONS,
    CRS,
    SNAP_TOLERANCE,
    DUPLICATE_TOLERANCE,
    KEEP_POTENTIAL_PROJECT,
    DROP_SNAP2018,
)


QA = True


data_dir = Path("../data/sarp/")
nhd_dir = data_dir / "derived/nhd/region"
sarp_dir = data_dir / "inventory"
out_dir = data_dir / "derived/inputs"
qa_dir = data_dir / "qa"
barriers_filename = "Road_Related_Barriers_DraftOne_Final08012019.gdb"

start = time()


all_sb = gp.read_file(sarp_dir / barriers_filename).rename(
    columns={"AnalysisId": "AnalysisID"}
)

print("Read {} small barriers".format(len(all_sb)))

if QA:
    orig_df = all_sb.copy()


### Filter out small barriers by Potential_Project and SNAP2018

# NOTE: small barriers currently do not have any values set for SNAP2018
all_sb = all_sb.loc[
    all_sb.Potential_Project.isin(KEEP_POTENTIAL_PROJECT)
    & ~all_sb.SNAP2018.isin(DROP_SNAP2018)
][["AnalysisID", "HUC12", "geometry"]].to_crs(CRS)
print("{} small barriers left after filtering".format(len(all_sb)))

if QA:
    orig_df.loc[~orig_df.index.isin(all_sb.index), "dropped"] = "drop Potential_Project"


### Remove duplicates (within DUPLICATE_TOLERANCE)
all_sb = remove_duplicates(all_sb, DUPLICATE_TOLERANCE)
print("{} small barriers left after removing duplicates".format(len(all_sb)))

if QA:
    orig_df.loc[
        ~orig_df.index.isin(all_sb.index) & orig_df.dropped.isnull(), "dropped"
    ] = "drop duplicate"

all_sb["joinID"] = all_sb.AnalysisID

# NOTE: these currently include HUC12, which we use for deriving HUC2
# Long term, this may need to be replaced with a spatial join here
all_sb["HUC2"] = all_sb.HUC12.str[:2]


snapped = None

for group in REGION_GROUPS:
    print("\n----- {} ------\n".format(group))

    region_dir = nhd_dir / group

    sb = all_sb.loc[all_sb.HUC2.isin(REGION_GROUPS[group])].copy()
    print("Selected {0} small barriers in region {1}".format(len(sb), group))

    if len(sb):
        print("Reading flowlines")
        flowlines = deserialize_gdf(region_dir / "flowline.feather").set_index(
            "lineID", drop=False
        )
        print("Read {0} flowlines".format(len(flowlines)))

        print("Reading spatial index on flowlines")
        sindex = deserialize_sindex(region_dir / "flowline.sidx")

        print("Snapping small barriers")
        sb = snap_to_line(sb, flowlines, SNAP_TOLERANCE, sindex=sindex)
        print("{} small barriers were successfully snapped".format(len(sb)))

        if snapped is None:
            snapped = sb
        else:
            snapped = snapped.append(sb, sort=False, ignore_index=True)

print("\n--------------\n")
print("Serializing {0} snapped barriers out of {1}".format(len(snapped), len(all_sb)))
serialize_gdf(snapped, out_dir / "snapped_small_barriers.feather", index=False)
print("Done in {:.2f}".format(time() - start))

if QA:
    # Write out those that didn't snap for QA
    print("writing shapefiles for QA/QC")

    to_shp(
        orig_df.loc[~orig_df.dropped.isnull()], qa_dir / "dropped_small_barriers.shp"
    )

    to_shp(snapped, qa_dir / "snapped_small_barriers.shp")
    to_shp(
        all_sb.loc[~all_sb.joinID.isin(snapped.joinID)],
        qa_dir / "unsnapped_small_barriers.shp",
    )

