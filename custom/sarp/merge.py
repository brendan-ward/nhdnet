import os
from time import time
from nhdnet.io import serialize_gdf, deserialize_gdf

src_dir = "/Users/bcward/projects/data/sarp/nhd"


units = {
    "02": [7, 8],
    "03": list(range(1, 17)),
    "05": [5, 7, 9, 10, 11, 13, 14],
    "06": list(range(1, 5)),
    "07": [10, 11, 14],
    "10": [24, 28, 29, 30],
    "11": [1, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14],
    "12": list(range(1, 12)),
    "13": [3, 4, 5, 7, 8, 9],
}

HUC2 = "13"


start = time()


merged = None
for i in units[HUC2]:
    HUC4 = "{0}{1:02d}".format(HUC2, i)

    print("Processing {}".format(HUC4))

    huc_dir = "{0}/{1}".format(src_dir, HUC4)
    flowlines = deserialize_gdf("{}/flowline.feather".format(huc_dir))
    flowlines["HUC4"] = HUC4

    if merged is None:
        merged = flowlines
    else:
        merged = merged.append(flowlines, ignore_index=True)

region_dir = "{0}/{1}".format(src_dir, HUC2)
if not os.path.exists(region_dir):
    os.makedirs(region_dir)

print("serializing to feather")
serialize_gdf(merged, "{}/flowline.feather".format(region_dir))

print("serializing to shp")
merged.to_file("{}/flowline.shp".format(region_dir), driver="ESRI Shapefile")


print("Done in {:.2f}".format(time() - start))
