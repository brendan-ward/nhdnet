"""
Extract NHD File Geodatabases (FGDB) for all HUC4s within a region (HUC2).

Only the flowlines, joins between flowlines, and specific attributes are extracted for analysis.

Due to data limitations of the FGDB / Shapefile format, NHDPlus IDs are represented natively as float64 data.
However, float64 data are not ideal for indexing, so all IDs are converted to uint64 within this package, and
converted back to float64 only for export to GIS.

These are output as 2 files:
* flowlines.feather: serialized flowline geometry and attributes
* flowline_joins.feather: serialized joins between adjacent flowlines, with the upstream and downstream IDs of a join

Note: there are cases where Geopandas is unable to read a FGDB file.  See `nhdnet.nhd.extract` for specific workarounds.
"""

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

    flowlines.FType = flowlines.FType.astype("uint16")
    flowlines.StreamOrde = flowlines.StreamOrde.astype("uint8")

    print("Extract Done in {:.2f}".format(time() - start))

    print("Serializing flowlines geo file")
    serialize_gdf(flowlines, "{}/flowline.feather".format(out_dir), index=False)

    print("Writing segment connections")
    serialize_df(joins, "{}/flowline_joins.feather".format(out_dir), index=False)


    # If a shapefile is needed - need to convert ID back to a float
    # geo_df = flowlines.copy()
    # geo_df.NHDPlusID = geo_df.NHDPlusID.astype("float64")
    # to_shp(geo_df, "{}/flowline.shp".format(out_dir))

    print("Serializing Done in {:.2f}".format(time() - shp_start))

    print("Done in {:.2f}\n============================".format(time() - start))
