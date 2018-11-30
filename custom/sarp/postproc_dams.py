import pandas as pd
import geopandas as gp

from nhdnet.io import serialize_gdf, to_shp


HUC2 = "06"
working_dir = "/Users/bcward/projects/data/sarp/nhd/{0}".format(HUC2)

print("Reading dams")

# manually snapped dams have had some dams removed, so expect this not to match perfectly with QA
# remove any that are marked with a SNAP2018 of 5 (manually determined NOT on network)
dams = gp.read_file("{0}/dams_{1}_snapped.shp".format(working_dir, HUC2)).set_index(
    "AnalysisID"
)
dams = dams.loc[dams.SNAP2018 != 5].copy()

# QA dams that didn't snap have been removed; they were accidentally included in the above
qa = gp.read_file("{0}/dams_{1}_qa.shp".format(working_dir, HUC2)).set_index(
    "AnalysisID"
)
print("Started from {} dams".format(len(qa)))

dams = dams.join(qa[[]], how="inner")

print("Reduced to {} dams after join and filter".format(len(dams)))

serialize_gdf(dams, "{}/dams_post_qa.feather".format(working_dir))

