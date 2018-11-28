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
SNAP_TOLERANCE = 100  # meters - tolerance for waterfalls

src_dir = "/Users/bcward/projects/data/sarp/nhd"

HUC2 = "03"
# HUC4 = "0602"

start = time()


# gdb = "/Users/bcward/projects/data/sarp/All_Dams_Merge_SnappedandUnsnapped_11272018.gdb"
# layer = "AllDamsInventoryMerge_11272018_SnappedandUnsnapped_preschema"
# dams = gp.read_file(gdb, layer=layer)

all_wf = (
    gp.read_file("/Users/bcward/projects/data/sarp/Waterfalls_USGS_2017.gdb")
    .to_crs(CRS)
    .set_index("OBJECTID", drop=False)
)
all_wf["joinID"] = all_wf.OBJECTID
all_wf["HUC2"] = all_wf.HUC_8.str[:2]
# all_dams["HUC4"] = all_dams.HUC12.str[:4]

# Select out only the dams in this HUC
wf = all_wf.loc[all_wf.HUC2 == HUC2].copy()
print("selected {0} waterfalls in this unit".format(len(wf)))

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


snapper = snap_to_line(flowlines, SNAP_TOLERANCE, prefer_endpoint=False)
print("Snapping waterfalls")
snapped = wf.apply(snapper, axis=1)
wf = gp.GeoDataFrame(
    wf.drop(columns=["geometry"]).join(snapped), geometry="geometry", crs=flowlines.crs
)

print("writing shapefile")

# export to SHP for manual review and snapping
wf.to_file(
    "{0}/{1}/waterfalls_{1}_qa.shp".format(src_dir, HUC2), driver="ESRI Shapefile"
)

print("Done in {:.2f}".format(time() - start))
