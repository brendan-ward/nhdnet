import os

from nhdnet.nhd.extract import extract_flowlines

# Use USGS CONUS Albers (EPSG:102003): https://epsg.io/102003    (same as other SARP datasets)
# use Proj4 syntax, since GeoPandas doesn't properly recognize it's EPSG Code.
CRS = "+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=37.5 +lon_0=-96 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs"

src_dir = "/Users/bcward/projects/data/sarp/nhd"

for HUC4 in ("0602",):
    out_dir = "{0}/{1}".format(src_dir, HUC4)

    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    gdb = "{0}/NHDPLUS_H_{1}_HU4_GDB.gdb".format(src_dir, HUC4)
    flowlines, joins = extract_flowlines(gdb, target_crs=CRS)

    # Write to shapefile and CSV for easier processing later
    print("Writing flowlines to disk")
    flowlines.drop(columns=["geometry"]).to_csv(
        "{}/flowline.csv".format(out_dir), index=False
    )

    print("Writing segment connections")
    joins.to_csv("{}/flowline_joins.csv".format(out_dir), index=False)

    # Always write NHDPlusID back out as a float64
    print("Writing flowlines shapefile")
    geo_df = flowlines.copy()
    geo_df.NHDPlusID = geo_df.NHDPlusID.astype("float64")
    geo_df.to_file("{}/flowline.shp".format(out_dir), driver="ESRI Shapefile")
