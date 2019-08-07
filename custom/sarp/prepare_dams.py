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
    DROP_FEASIBILITY,
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

# TODO: use AnalysisID once that is fixed
all_dams["joinID"] = all_dams.index.astype("uint")
# all_dams["joinID"] = all_dams.AnalysisID


if QA:
    orig_df = all_dams.copy()

### Cleanup data
all_dams.Snap2018 = all_dams.Snap2018.fillna(0).astype("uint8")
all_dams.PotentialFeasibility = all_dams.PotentialFeasibility.fillna(0).astype("uint8")
all_dams.Recon = all_dams.Recon.fillna(0).astype("uint8")

# Fix Recon value that wasn't assigned to Snap2018
# these are invasive species barriers
all_dams.loc[all_dams.Recon == 16, "Snap2018"] = 10


### Filter out dams by Snap2018 and PotentialFeasibility
all_dams = all_dams.loc[
    ~(
        all_dams.Snap2018.isin(DROP_SNAP2018)
        | all_dams.PotentialFeasibility.isin(DROP_FEASIBILITY)
    )
][["joinID", "HUC12", "geometry"]].to_crs(CRS)
print("{} dams left after filtering".format(len(all_dams)))

if QA:
    orig_df.loc[~orig_df.index.isin(all_dams.index), "dropped"] = "drop SNAP2018"

### Remove duplicates (within DUPLICATE_TOLERANCE)
all_dams = remove_duplicates(all_dams, DUPLICATE_TOLERANCE)
print("{} dams left after removing duplicates".format(len(all_dams)))

if QA:
    orig_df.loc[
        ~orig_df.index.isin(all_dams.index) & orig_df.dropped.isnull(), "dropped"
    ] = "drop duplicate"


# NOTE: these data currently include HUC12, which we use for deriving HUC2
# Long term, this may need to be replaced with a spatial join here
all_dams["HUC2"] = all_dams.HUC12.str[:2]


snapped = None

for group in REGION_GROUPS:
    print("\n----- {} ------\n".format(group))

    region_dir = nhd_dir / group

    dams = all_dams.loc[all_dams.HUC2.isin(REGION_GROUPS[group])].copy()
    print("Selected {0} small barriers in region {1}".format(len(dams), group))

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

    orig_df.loc[
        orig_df.joinID.isin(dup.joinID), "dropped"
    ] = "drop duplicate after snapping"

snapped = dedup


print("\n--------------\n")
print("Serializing {0} snapped dams".format(len(snapped)))
serialize_gdf(snapped, out_dir / "snapped_dams.feather", index=False)
print("Done in {:.2f}".format(time() - start))

if QA:
    # Write out those that didn't snap for QA
    print("writing shapefiles for QA/QC")

    to_shp(orig_df.loc[~orig_df.dropped.isnull()], qa_dir / "dropped_dams.shp")

    to_shp(snapped, qa_dir / "snapped_dams.shp")
    to_shp(
        all_dams.loc[~all_dams.joinID.isin(snapped.joinID)],
        qa_dir / "unsnapped_dams.shp",
    )

