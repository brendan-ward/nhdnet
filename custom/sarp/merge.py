import os
from time import time
from nhdnet.io import serialize_gdf, deserialize_gdf

src_dir = "/Users/bcward/projects/data/sarp/nhd"

region = 6

start = time()


merged = None
for i in range(1, 5):
    HUC4 = "{0:02d}{1:02d}".format(region, i)

    print("Processing {}".format(HUC4))

    huc_dir = "{0}/{1}".format(src_dir, HUC4)
    flowlines = deserialize_gdf("{}/flowline.feather".format(huc_dir))
    flowlines["HUC4"] = HUC4

    if merged is None:
        merged = flowlines
    else:
        merged = merged.append(flowlines, ignore_index=True)

region_dir = "{0}/{1:02d}".format(src_dir, region)
if not os.path.exists(region_dir):
    os.makedirs(region_dir)

print("serializing to feather")
serialize_gdf(merged, "{}/flowline.feather".format(region_dir))

print("serializing to shp")
merged.to_file("{}/flowline.shp".format(region_dir), driver="ESRI Shapefile")


print("Done in {:.2f}".format(time() - start))
