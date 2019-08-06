from pathlib import Path
import os
from time import time

from nhdnet.io import serialize_gdf, deserialize_gdf, serialize_sindex

from constants import REGION_GROUPS


src_dir = Path("../data/sarp/derived/nhd/region")

start = time()

for group in REGION_GROUPS:
    if os.path.exists(src_dir / group / "flowline.sidx"):
        print("Skipping existing spatial index {}".format(group))
        continue

    group_start = time()
    print("-------- Group {} --------".format(group))

    print("Reading flowlines")
    flowlines = deserialize_gdf(src_dir / group / "flowline.feather").set_index(
        "lineID", drop=False
    )
    print("Read {0} flowlines".format(len(flowlines)))

    print("Creating spatial index on flowlines")
    serialize_sindex(flowlines, src_dir / group / "flowline.sidx")

    print("Group Done in {:.2f}".format(time() - group_start))

print("All Done in {:.2f}".format(time() - start))
