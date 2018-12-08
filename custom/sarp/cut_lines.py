import sys

sys.path.append("/Applications/PyVmMonitor.app/Contents/MacOS/public_api")
import pyvmmonitor

pyvmmonitor.connect()


import os
from time import time

import geopandas as gp
import pandas as pd
from shapely.geometry import MultiLineString

from nhdnet.geometry.points import create_points
from nhdnet.geometry.lines import snap_to_line

from nhdnet.nhd.cut import cut_flowlines
from nhdnet.nhd.network import generate_network, calculate_network_stats
from nhdnet.io import (
    deserialize_df,
    deserialize_gdf,
    to_shp,
    serialize_df,
    serialize_gdf,
)

HUC2 = "06"
src_dir = "/Users/bcward/projects/data/sarp"
working_dir = "{0}/nhd/{1}".format(src_dir, HUC2)
os.chdir(working_dir)

start = time()

##################### Read NHD data #################
print("reading flowlines")
flowlines = deserialize_gdf("flowline.feather").set_index("lineID", drop=False)
print("read {} flowlines".format(len(flowlines)))

print("reading barriers")
barriers = deserialize_gdf("barriers.feather").set_index("joinID", drop=False)
print("read {} barriers".format(len(barriers)))

print("reading joins")
joins = deserialize_df("flowline_joins.feather")

print("Starting cutting from {} original segments".format(len(flowlines)))

cut_start = time()

next_segment_id = (
    int(HUC2) * 1000000 + 1
)  # since all other lineIDs use HUC4s, this should be unique
flowlines, joins, barrier_joins = cut_flowlines(
    flowlines, barriers, joins, next_segment_id=next_segment_id
)
# barrier_joins.upstream_id = barrier_joins.upstream_id.astype("uint32")
# barrier_joins.downstream_id = barrier_joins.downstream_id.astype("uint32")

print("Done cutting in {:.2f}".format(time() - cut_start))
