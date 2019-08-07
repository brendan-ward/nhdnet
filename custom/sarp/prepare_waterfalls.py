"""
Extract waterfalls from original data source, process for use in network analysis, and convert to feather format.
1. Remove records with bad coordinates (one waterfall was represented in wrong projection)
2. Cleanup data values (as needed)
3. Snap to networks by HUC2 and merge into single data frame
"""

from pathlib import Path
import pandas as pd
import geopandas as gp

from nhdnet.io import serialize_gdf, deserialize_gdf, deserialize_sindex, to_shp
from nhdnet.geometry.lines import snap_to_line
from nhdnet.geometry.points import remove_duplicates

from constants import REGION_GROUPS, REGIONS, CRS, SNAP_TOLERANCE, DUPLICATE_TOLERANCE


QA = True
# Set to True to output shapefiles for QA/QC


data_dir = Path("../data/sarp/")
nhd_dir = data_dir / "derived/nhd/region"
wbd_dir = data_dir / "nhd/wbd"
sarp_dir = data_dir / "inventory"
out_dir = data_dir / "derived/inputs"
qa_dir = data_dir / "qa"
gdb_filename = "Waterfalls2019.gdb"


print("Reading waterfalls")

all_wf = gp.read_file(sarp_dir / gdb_filename)[
    ["geometry", "Source", "fall_id", "LocalID"]
]

all_wf["joinID"] = all_wf.index.astype("uint")

if QA:
    orig_df = all_wf.copy()


### Drop records with bad coordinates
all_wf = all_wf.loc[all_wf.geometry.y.abs() <= 90]

if QA:
    orig_df.loc[orig_df.geometry.y.abs() > 90, "dropped"] = "bad coordinates"

### Cleanup data
all_wf.Source = all_wf.Source.str.strip()
amy_idx = all_wf.Source == "Amy Cottrell, Auburn"
all_wf.loc[amy_idx, "Source"] = "Amy Cotrell, Auburn University"

### Add persistant sourceID based on original IDs
all_wf["sourceID"] = all_wf.LocalID
usgs_idx = ~all_wf.fall_id.isnull()
all_wf.loc[usgs_idx, "sourceID"] = (
    all_wf.loc[usgs_idx].fall_id.astype("int").astype("str")
)

### Assign HUC2
print("Reading HUC2 boundaries and joining to waterfalls")
huc2 = gp.read_file(wbd_dir / "HUC2/HUC2.shp")
all_wf.sindex
huc2.sindex
all_wf = gp.sjoin(all_wf, huc2[["geometry", "HUC2"]], how="left").drop(
    columns=["index_right"]
)

### Reproject to CONUS Albers
all_wf = all_wf.to_crs(CRS)

print("Starting snapping for {} waterfalls".format(len(all_wf)))
snapped = None

for group in REGION_GROUPS:
    print("\n----- {} ------\n".format(group))
    src_dir = nhd_dir / group

    print("Reading flowlines")
    flowlines = deserialize_gdf(src_dir / "flowline.feather").set_index(
        "lineID", drop=False
    )
    print("Read {0} flowlines".format(len(flowlines)))

    print("Reading spatial index on flowlines")
    sindex = deserialize_sindex(src_dir / "flowline.sidx")

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
print("Serializing {0} snapped waterfalls out of {1}".format(len(snapped), len(all_wf)))
serialize_gdf(snapped, out_dir / "snapped_waterfalls.feather", index=False)


if QA:
    to_shp(orig_df.loc[~orig_df.dropped.isnull()], qa_dir / "dropped_waterfalls.shp")
    # Write out those that didn't snap for QA
    print("writing shapefiles for QA/QC")
    to_shp(snapped, qa_dir / "snapped_waterfalls.shp")

    unsnapped = all_wf.loc[~all_wf.joinID.isin(snapped.joinID)]
    to_shp(unsnapped, qa_dir / "unsnapped_waterfalls.shp")

