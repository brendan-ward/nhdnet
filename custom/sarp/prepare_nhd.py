import os
from time import time

from nhdnet.nhd.extract import extract_flowlines
from nhdnet.io import serialize_gdf, serialize_df, to_shp

from constants import REGIONS, CRS

src_dir = "/Users/bcward/projects/data/sarp/nhd"

HUC2 = "03"

for i in REGIONS[HUC2]:
    start = time()

    HUC4 = "{0}{1:02d}".format(HUC2, i)
    print("Processing {}".format(HUC4))
    out_dir = "{0}/{1}".format(src_dir, HUC4)

    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    gdb = "{0}/NHDPLUS_H_{1}_HU4_GDB/NHDPLUS_H_{1}_HU4_GDB.gdb".format(src_dir, HUC4)
    flowlines, joins = extract_flowlines(gdb, target_crs=CRS)

    print("Extract Done in {:.2f}".format(time() - start))

    # Write to shapefile and CSV for easier processing later
    print("Writing flowlines to disk")
    serialize_df(
        flowlines.drop(columns=["geometry"]), "{}/flowline_data.feather".format(out_dir)
    )
    # flowlines.drop(columns=["geometry"]).to_csv(
    #     "{}/flowline.csv".format(out_dir), index=False
    # )

    print("Writing segment connections")
    serialize_df(joins, "{}/flowline_joins.feather".format(out_dir), index=False)
    # joins.to_csv("{}/flowline_joins.csv".format(out_dir), index=False)
    print("CSVs done in {:.2f}".format(time() - start))

    shp_start = time()
    # Always write NHDPlusID back out as a float64
    print("Serializing flowlines geo file")
    serialize_gdf(flowlines, "{}/flowline.feather".format(out_dir), index=False)

    # If a shapefile is needed
    # geo_df = flowlines.copy()
    # geo_df.NHDPlusID = geo_df.NHDPlusID.astype("float64")
    # to_shp(geo_df, "{}/flowline.shp".format(out_dir))

    print("Serializing Done in {:.2f}".format(time() - shp_start))

    print("Done in {:.2f}\n============================".format(time() - start))
