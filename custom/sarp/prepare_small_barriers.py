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
import numpy as np

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
    DROP_POTENTIAL_PROJECT,
    DROP_SNAP2018,
    EXCLUDE_SNAP2018,
)


QA = True
# Set to True to output shapefiles for QA/QC


data_dir = Path("../data/sarp/")
nhd_dir = data_dir / "derived/nhd/region"
sarp_dir = data_dir / "inventory"
out_dir = data_dir / "derived/inputs"
qa_dir = data_dir / "qa"
barriers_filename = "Road_Related_Barriers_DraftOne_Final08012019.gdb"

start = time()


all_sb = (
    gp.read_file(sarp_dir / barriers_filename)
    .rename(columns={"AnalysisId": "AnalysisID"})
    .to_crs(CRS)
)

print("Read {} small barriers".format(len(all_sb)))


# joinID is used for all internal joins in analysis
all_sb["joinID"] = all_sb.index.astype("uint")

# NOTE: these currently include HUC12, which we use for deriving HUC2
# Long term, this may need to be replaced with a spatial join here
all_sb["HUC2"] = all_sb.HUC12.str[:2]


if QA:
    qa_df = all_sb.copy()
    qa_df["dropped"] = np.nan


### Add tracking fields
all_sb["networkAnalysis"] = False
all_sb["snapped"] = False


### Filter out any small barriers that should be completely dropped from analysis
# NOTE: small barriers currently do not have any values set for SNAP2018
drop_idx = all_sb.Potential_Project.isin(DROP_POTENTIAL_PROJECT) | all_sb.SNAP2018.isin(
    DROP_SNAP2018
)
print(
    "Dropped {} small barriers from all analysis and mapping".format(
        len(all_sb.loc[drop_idx])
    )
)
all_sb = all_sb.loc[~drop_idx].copy()


if QA:
    qa_df.loc[~qa_df.joinID.isin(all_sb), "dropped"] = "drop Potential_Project"


### Exclude barriers that should not be analyzed or prioritized
# NOTE: small barriers currently do not have any values set for SNAP2018
keep_idx = all_sb.Potential_Project.isin(
    KEEP_POTENTIAL_PROJECT
) & ~all_sb.SNAP2018.isin(EXCLUDE_SNAP2018)

all_sb.loc[keep_idx, "networkAnalysis"] = True
print(
    "Excluded {} small barriers from network analysis and prioritization".format(
        len(all_sb) - len(all_sb.loc[keep_idx])
    )
)

if QA:
    qa_df.loc[
        (~qa_df.joinID.isin(all_sb.loc[keep_idx].joinID)) & qa_df.dropped.isnull(),
        "dropped",
    ] = "exclude Potential_Project"


### Remove duplicates (within DUPLICATE_TOLERANCE)
# These are completely dropped from the analysis from here on out
all_sb = remove_duplicates(all_sb, DUPLICATE_TOLERANCE)
print("{} small barriers left after removing duplicates".format(len(all_sb)))

if QA:
    qa_df.loc[
        ~qa_df.joinID.isin(all_sb.joinID) & qa_df.dropped.isnull(), "dropped"
    ] = "drop duplicate"

### Snap by region group
to_snap = all_sb.loc[all_sb.networkAnalysis, ["joinID", "geometry"]].copy()

snapped = None
for group in REGION_GROUPS:
    print("\n----- {} ------\n".format(group))

    region_dir = nhd_dir / group

    sb = to_snap.loc[all_sb.HUC2.isin(REGION_GROUPS[group])].copy()
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


# Remove duplicates after snapping, in case any snapped to the same position
# These are completely dropped from the analysis from here on out
dedup = remove_duplicates(snapped, DUPLICATE_TOLERANCE)

if QA:
    dup = snapped.loc[~snapped.joinID.isin(dedup.joinID)]
    print("Removed {} duplicates after snapping".format(len(dup)))

    qa_df.loc[
        qa_df.joinID.isin(dup.joinID), "dropped"
    ] = "drop duplicate after snapping"

snapped = dedup

### Update snapped geometry into master
all_sb = (
    all_sb.drop(columns=["networkAnalysis"])
    .set_index("joinID")
    .join(
        snapped.set_index("joinID")[
            ["geometry", "lineID", "NHDPlusID", "snap_dist", "nearby"]
        ],
        rsuffix="_snapped",
    )
    .reset_index()
)
idx = ~all_sb.geometry_snapped.isnull()
all_sb.loc[idx, "geometry"] = all_sb.loc[idx].geometry_snapped
all_sb.loc[idx, "snapped"] = True
all_sb = all_sb.drop(columns=["geometry_snapped"])


print("\n--------------\n")
print(
    "Serializing {0} snapped small barriers out of {1}".format(
        len(all_sb.loc[all_sb.snapped]), len(all_sb)
    )
)
serialize_gdf(all_sb, out_dir / "small_barriers.feather", index=False)

if QA:
    print("writing shapefiles for QA/QC")
    to_shp(all_sb, qa_dir / "small_barriers.shp")

    # write out any that were dropped by the analysis
    to_shp(qa_df.loc[~qa_df.dropped.isnull()], qa_dir / "dropped_small_barriers.shp")

