import os
from time import time

from nhdnet.io import serialize_gdf, deserialize_gdf, serialize_sindex

from constants import REGION_GROUPS


src_dir = "/Users/bcward/projects/data/sarp/nhd/region"

start = time()

for group in REGION_GROUPS:
    group_start = time()
    print("-------- Group {} --------".format(group))

    os.chdir("{0}/{1}".format(src_dir, group))

    print("Reading flowlines")
    flowlines = deserialize_gdf("flowline.feather").set_index("lineID", drop=False)
    print("Read {0} flowlines".format(len(flowlines)))

    print("Creating spatial index on flowlines")
    serialize_sindex(flowlines, "flowline.sidx")

    print("Group Done in {:.2f}".format(time() - group_start))

print("All Done in {:.2f}".format(time() - start))
