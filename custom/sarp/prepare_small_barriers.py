import os
from time import time
import pandas as pd
import geopandas as gp

from nhdnet.io import serialize_gdf

# Use USGS CONUS Albers (EPSG:102003): https://epsg.io/102003    (same as other SARP datasets)
# use Proj4 syntax, since GeoPandas doesn't properly recognize it's EPSG Code.
CRS = "+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=37.5 +lon_0=-96 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs"

src_dir = "/Users/bcward/projects/data/sarp"
os.chdir(src_dir)

df = gp.read_file(
    "Blank_Schema_Road_Barriers_WebViewer_DraftTwo.gdb",
    layer="Road_Barriers_WebViewer_Metrics_Schema_11272018",
).rename(columns={"AnalysisId": "AnalysisID"})

print("Read {} small barriers".format(len(df)))

# Filter by Potential_Project, based on guidance from Kat
keep = [
    "Severe Barrier",
    "Moderate Barrier",
    "Inaccessible",
    "Significant Barrier",
    "No Upstream Channel",
    "Indeterminate",
    "Potential Project",
    "Proposed Project",
]

df = df.loc[df.Potential_Project.isin(keep)][
    ["AnalysisID", "HUC12", "geometry"]
].to_crs(CRS)

print("{} small barriers left after filtering".format(len(df)))

serialize_gdf(df, "small_barriers.feather", index=False)

