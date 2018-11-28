"""
Use ogr2ogr first to convert CRS and extract out fields we want:

ogr2ogr -t_srs "EPSG:102003" -f "ESRI Shapefile" dams_11272018.shp All_Dams_Merge_SnappedandUnsnapped_11272018.gdb AllDamsInventoryMerge_11272018_SnappedandUnsnapped_preschema -sql "SELECT AnalysisID, SNAP2018, Barrier_Name as Name, River, HUC12 from AllDamsInventoryMerge_11272018_SnappedandUnsnapped_preschema"

ogr2ogr -t_srs "EPSG:102003" -f "ESRI Shapefile" dams_11272018.shp All_Dams_Merge_SnappedandUnsnapped_11272018_2.gdb AllDamsInventoryMerge_11272018_Tosnap_preschema_HUC -sql "SELECT AnalysisID, SNAP2018, Barrier_Name as Name, River, HUC12 from AllDamsInventoryMerge_11272018_Tosnap_preschema_HUC"
"""


import os
from time import time

import geopandas as gp
import pandas as pd
from fuzzywuzzy import fuzz

from nhdnet.geometry.points import create_points
from nhdnet.geometry.lines import snap_to_line
from nhdnet.io import deserialize_gdf

CRS = "+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=37.5 +lon_0=-96 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs"
SNAP_TOLERANCE_DAMS = 200  # meters  FIXME: should be 100m
SNAP_TOLERANCE = 100  # meters - tolerance for waterfalls

src_dir = "/Users/bcward/projects/data/sarp/nhd"

HUC2 = "13"
# HUC4 = "0602"

start = time()


# gdb = "/Users/bcward/projects/data/sarp/All_Dams_Merge_SnappedandUnsnapped_11272018.gdb"
# layer = "AllDamsInventoryMerge_11272018_SnappedandUnsnapped_preschema"
# dams = gp.read_file(gdb, layer=layer)

all_dams = gp.read_file("/Users/bcward/projects/data/sarp/dams_11272018.shp").set_index(
    "AnalysisID", drop=False
)
all_dams["joinID"] = all_dams.AnalysisID
all_dams["HUC2"] = all_dams.HUC12.str[:2]
# all_dams["HUC4"] = all_dams.HUC12.str[:4]

# Select out only the dams in this HUC
dams = all_dams.loc[all_dams.HUC2 == HUC2].copy()
print("selected {0} dams in this unit".format(len(dams)))

print("Reading flowlines")
flowlines = deserialize_gdf("{0}/{1}/flowline.feather".format(src_dir, HUC2))[
    [
        "lineID",
        "NHDPlusID",
        "FType",
        "GNIS_ID",
        "GNIS_Name",
        "StreamOrde",
        "sizeclass",
        "geometry",
    ]
]

snapper = snap_to_line(flowlines, SNAP_TOLERANCE_DAMS, prefer_endpoint=False)
print("Snapping dams")
snapped = dams.geometry.apply(snapper)
dams = gp.GeoDataFrame(
    dams[["AnalysisID", "SNAP2018", "Name", "River"]].join(snapped),
    geometry="geometry",
    crs=flowlines.crs,
)

# now try and fuzzy match on river name
name_cols = ["River", "GNIS_Name"]
names = dams.loc[~(dams.River.isnull() | dams.GNIS_Name.isnull())][name_cols]
for col in name_cols:
    names[col] = names[col].str.lower()
    # names[col] = names[col].apply(lambda n: n.lower().replace(' creek', '').replace(' river', '').replace(' branch', '').replace(' stream', ))

names["fuzzmatch"] = names.apply(
    lambda row: fuzz.ratio(row.River, row.GNIS_Name), axis=1
)
names.loc[names.fuzzmatch > 90, "confidence"] = "high"

dams = dams.join(names[["fuzzmatch", "confidence"]])
dams.fuzzmatch = dams.fuzzmatch.fillna(0)
dams.confidence = dams.confidence.fillna("low")

print("writing shapefile")

# export to SHP for manual review and snapping
dams.to_file("{0}/{1}/dams_{1}_qa.shp".format(src_dir, HUC2), driver="ESRI Shapefile")

print("Done in {:.2f}".format(time() - start))
