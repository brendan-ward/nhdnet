"""
Extract waterfalls from original data source, process for use in network analysis, and convert to feather format.
1. Remove records with bad coordinates (one waterfall was represented in wrong projection)
2. Cleanup data values (as needed)
3. Snap to networks by HUC2 and merge into single data frame
"""

from pathlib import Path
import pandas as pd
import geopandas as gp
import numpy as np

from nhdnet.io import serialize_gdf, deserialize_gdf, deserialize_sindex, to_shp
from nhdnet.geometry.lines import snap_to_line
from nhdnet.geometry.points import remove_duplicates

from constants import REGION_GROUPS, REGIONS, CRS, SNAP_TOLERANCE, DUPLICATE_TOLERANCE


QA = True
# Set to True to output shapefiles for QA/QC


data_dir = Path("../data/sarp/")
nhd_dir = data_dir / "derived/nhd/region"
boundaries_dir = data_dir / "derived/boundaries"
sarp_dir = data_dir / "inventory"
out_dir = data_dir / "derived/inputs"
qa_dir = data_dir / "qa"
gdb_filename = "Waterfalls2019.gdb"


print("Reading waterfalls")

all_wf = gp.read_file(sarp_dir / gdb_filename)

# joinID is used for all internal joins in analysis
all_wf["joinID"] = all_wf.index.astype("uint")

if QA:
    qa_df = all_wf.copy()


### Add tracking fields
all_wf["networkAnalysis"] = True  # Note: none are filtered out currently
all_wf["snapped"] = False

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


### Drop records with bad coordinates
# must be done before projecting coordinates
all_wf = all_wf.loc[all_wf.geometry.y.abs() <= 90]

### Reproject to CONUS Albers
all_wf = all_wf.to_crs(CRS)


print("Reading HUC2 boundaries and joining to waterfalls")
huc12 = deserialize_gdf(boundaries_dir / "HUC12.feather")
all_wf.sindex
huc12.sindex
all_wf = gp.sjoin(all_wf, huc12, how="left").drop(columns=["index_right"])


print("Reading state boundaries and joining to waterfalls")
states = deserialize_gdf(boundaries_dir / "states.feather")
states.sindex
all_wf = gp.sjoin(all_wf, states, how="left").drop(columns=["index_right"])


if QA:
    qa_df = all_wf.copy()
    qa_df["dropped"] = np.nan
    qa_df.loc[
        all_wf.HUC12.isnull() | all_wf.STATEFIPS.isnull(), "dropped"
    ] = "dropped outside of HUC12 or states"


# Drop any that didn't intersect HUCs or states
print(
    "{} small barriers are outside HUC12 / states".format(
        len(all_wf.loc[all_wf.HUC12.isnull() | all_wf.STATEFIPS.isnull()])
    )
)
all_wf = all_wf.dropna(subset=["HUC12", "STATEFIPS"])


# Add HUC2 from HUC12
all_wf["HUC2"] = all_wf.HUC12.str[:2]


### Snap by region group
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


### Remove duplicates after snapping, in case any snapped to the same position
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
all_wf = (
    all_wf.set_index("joinID")
    .join(
        snapped.set_index("joinID")[
            ["geometry", "lineID", "NHDPlusID", "snap_dist", "nearby"]
        ],
        rsuffix="_snapped",
    )
    .reset_index()
)
idx = all_wf.loc[~all_wf.geometry_snapped.isnull()].index
all_wf.loc[idx, "geometry"] = all_wf.loc[idx].geometry_snapped
all_wf.loc[idx, "snapped"] = True
all_wf = all_wf.drop(columns=["geometry_snapped"])


print("\n--------------\n")
print(
    "Serializing {0} snapped waterfalls out of {1}".format(
        len(all_wf.loc[all_wf.snapped]), len(all_wf)
    )
)
serialize_gdf(all_wf, out_dir / "waterfalls.feather", index=False)

if QA:
    print("writing shapefiles for QA/QC")
    to_shp(all_wf, qa_dir / "waterfalls.shp")

    # write out any that were dropped by the analysis
    to_shp(qa_df.loc[~qa_df.dropped.isnull()], qa_dir / "dropped_waterfalls.shp")
