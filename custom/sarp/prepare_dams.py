"""
Extract dams from original data source, process for use in network analysis, and convert to feather format.
1. Cleanup data values (as needed)
2. Filter out dams not to be included in analysis (based on PotentialFeasibility and Snap2018)
3. Remove duplicate dams
4. Snap to networks by HUC2 and merge into single data frame
"""

from pathlib import Path
from time import time
import pandas as pd
import geopandas as gp
import numpy as np

from nhdnet.io import serialize_gdf, deserialize_gdf, deserialize_sindex, to_shp
from nhdnet.geometry.lines import snap_to_line
from nhdnet.geometry.points import remove_duplicates

from constants import (
    REGION_GROUPS,
    REGIONS,
    CRS,
    SNAP_TOLERANCE,
    DUPLICATE_TOLERANCE,
    DROP_SNAP2018,
    EXCLUDE_SNAP2018,
    DROP_FEASIBILITY,
    EXCLUDE_FEASIBILITY,
)


QA = True


data_dir = Path("../data/sarp/")
nhd_dir = data_dir / "derived/nhd/region"
sarp_dir = data_dir / "inventory"
out_dir = data_dir / "derived/inputs"
qa_dir = data_dir / "qa"
dams_filename = "Dams_Webviewer_DraftOne_Final.gdb"


start = time()

all_dams = gp.read_file(sarp_dir / dams_filename)
print("Read {} dams".format(len(all_dams)))


all_dams["joinID"] = all_dams.index.astype("uint")

# NOTE: these data currently include HUC12, which we use for deriving HUC2
# Long term, this may need to be replaced with a spatial join here
all_dams["HUC2"] = all_dams.HUC12.str[:2]


if QA:
    qa_df = all_dams.copy()
    qa_df["dropped"] = np.nan

### Add tracking fields
all_dams["networkAnalysis"] = False
all_dams["snapped"] = False


### Cleanup data
all_dams.Snap2018 = all_dams.Snap2018.fillna(0).astype("uint8")
all_dams.PotentialFeasibility = all_dams.PotentialFeasibility.fillna(0).astype("uint8")
all_dams.Recon = all_dams.Recon.fillna(0).astype("uint8")

# Fix Recon value that wasn't assigned to Snap2018
# these are invasive species barriers
all_dams.loc[all_dams.Recon == 16, "Snap2018"] = 10


### Filter out any dams that should be completely dropped from analysis
drop_idx = all_dams.PotentialFeasibility.isin(
    DROP_FEASIBILITY
) | all_dams.Snap2018.isin(DROP_SNAP2018)

print(
    "Dropped {} dams from all analysis and mapping".format(len(all_dams.loc[drop_idx]))
)
all_dams = all_dams.loc[~drop_idx].copy()

if QA:
    qa_df.loc[
        ~qa_df.joinID.isin(all_dams.joinID), "dropped"
    ] = "drop Snap2018 / PotentialFeasibility"


### Exclude dams that should not be analyzed or prioritized
exclude_idx = all_dams.Snap2018.isin(
    EXCLUDE_SNAP2018
) | all_dams.PotentialFeasibility.isin(EXCLUDE_FEASIBILITY)

all_dams.loc[~exclude_idx, "networkAnalysis"] = True
print(
    "Excluded {} dams from network analysis and prioritization".format(
        len(all_dams.loc[exclude_idx])
    )
)

if QA:
    qa_df.loc[
        qa_df.joinID.isin(all_dams.loc[exclude_idx].joinID) & qa_df.dropped.isnull(),
        "dropped",
    ] = "exclude Snap2018 / PotentialFeasibility"


### Project to CRS
all_dams = all_dams.to_crs(CRS)


### Remove duplicates (within DUPLICATE_TOLERANCE)
all_dams = remove_duplicates(all_dams, DUPLICATE_TOLERANCE)
print("{} dams left after removing duplicates".format(len(all_dams)))

if QA:
    qa_df.loc[
        ~qa_df.joinID.isin(all_dams.joinID) & qa_df.dropped.isnull(), "dropped"
    ] = "drop duplicate"


### Snap by region group
to_snap = all_dams.loc[all_dams.networkAnalysis, ["joinID", "geometry"]].copy()
snapped = None

for group in REGION_GROUPS:
    print("\n----- {} ------\n".format(group))

    region_dir = nhd_dir / group

    dams = to_snap.loc[all_dams.HUC2.isin(REGION_GROUPS[group])].copy()
    print("Selected {0} dams in region {1}".format(len(dams), group))

    print("Reading flowlines")
    flowlines = deserialize_gdf(region_dir / "flowline.feather").set_index(
        "lineID", drop=False
    )
    print("Read {0} flowlines".format(len(flowlines)))

    print("Reading spatial index on flowlines")
    sindex = deserialize_sindex(region_dir / "flowline.sidx")

    print("Snapping dams")
    dams = snap_to_line(dams, flowlines, SNAP_TOLERANCE, sindex=sindex)
    print("{} dams were successfully snapped".format(len(dams)))

    if snapped is None:
        snapped = dams

    else:
        snapped = snapped.append(dams, sort=False, ignore_index=True)


# Remove duplicates after snapping, in case any snapped to the same position
dedup = remove_duplicates(snapped, DUPLICATE_TOLERANCE)

if QA:
    dup = snapped.loc[~snapped.joinID.isin(dedup.joinID)]
    print("Removed {} duplicates after snapping".format(len(dup)))

    qa_df.loc[
        qa_df.joinID.isin(dup.joinID), "dropped"
    ] = "drop duplicate after snapping"

snapped = dedup


### Update snapped geometry into master
all_dams = (
    all_dams.drop(columns=["networkAnalysis"])
    .set_index("joinID")
    .join(
        snapped.set_index("joinID")[
            ["geometry", "lineID", "NHDPlusID", "snap_dist", "nearby"]
        ],
        rsuffix="_snapped",
    )
    .reset_index()
)
idx = ~all_dams.geometry_snapped.isnull()
all_dams.loc[idx, "geometry"] = all_dams.loc[idx].geometry_snapped
all_dams.loc[idx, "snapped"] = True
all_dams = all_dams.drop(columns=["geometry_snapped"])


print("\n--------------\n")
print(
    "Serializing {0} snapped dams out of {1}".format(
        len(all_dams.loc[all_dams.snapped]), len(all_dams)
    )
)
serialize_gdf(all_dams, out_dir / "dams.feather", index=False)

if QA:
    print("writing shapefiles for QA/QC")
    to_shp(all_dams, qa_dir / "dams.shp")

    # write out any that were dropped by the analysis
    to_shp(qa_df.loc[~qa_df.dropped.isnull()], qa_dir / "dropped_dams.shp")

