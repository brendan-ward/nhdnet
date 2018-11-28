import os
from time import time

from nhdnet.nhd.extract import extract_flowlines
from nhdnet.io import serialize_gdf, serialize_df

# Use USGS CONUS Albers (EPSG:102003): https://epsg.io/102003    (same as other SARP datasets)
# use Proj4 syntax, since GeoPandas doesn't properly recognize it's EPSG Code.
CRS = "+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=37.5 +lon_0=-96 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs"

src_dir = "/Users/bcward/projects/data/sarp/nhd"

# "0601", "0602", "0603", "0604"

# for HUC4 in ("0301", "0302", "0304", "0305", "0306", "0307", "0308"):
# region
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

HUC2 = "02"

for i in units[HUC2]:
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
    # geo_df[["geometry", "NHDPlusID", "lineID"]].to_file(
    #     "{}/flowline.shp".format(out_dir), driver="ESRI Shapefile"
    # )

    print("Serializing Done in {:.2f}".format(time() - shp_start))

    print("Done in {:.2f}\n============================".format(time() - start))
