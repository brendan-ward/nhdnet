"""
Extract NHD File Geodatabases (FGDB) for all HUC4s within a region (HUC2).

Data are downloaded using `nhd/download.py::download_huc4`.

Only the flowlines, joins between flowlines, and specific attributes are extracted for analysis.

Due to data limitations of the FGDB / Shapefile format, NHDPlus IDs are represented natively as float64 data.
However, float64 data are not ideal for indexing, so all IDs are converted to uint64 within this package, and
converted back to float64 only for export to GIS.

These are output as 2 files:
* flowlines.feather: serialized flowline geometry and attributes
* flowline_joins.feather: serialized joins between adjacent flowlines, with the upstream and downstream IDs of a join

Note: there are cases where Geopandas is unable to read a FGDB file.  See `nhdnet.nhd.extract` for specific workarounds.
"""

from pathlib import Path
import os
from time import time

from nhdnet.nhd.extract import extract_flowlines
from nhdnet.io import serialize_gdf, serialize_df, to_shp

from constants import REGIONS, CRS

src_dir = Path("../data/sarp/nhd/raw_nhd_2019")
out_dir = Path("../data/sarp/derived/nhd/huc4")

for HUC2 in REGIONS:
    for i in REGIONS[HUC2]:
        HUC4 = "{0}{1:02d}".format(HUC2, i)

        if os.path.exists(out_dir / HUC4 / "flowline.feather"):
            print("Skipping existing HUC4: {}".format(HUC4))
            continue

        print("Processing {}".format(HUC4))
        start = time()

        if not os.path.exists(out_dir / HUC4):
            os.makedirs(out_dir / HUC4)

        gdb = src_dir / HUC4 / "NHDPLUS_H_{HUC4}_HU4_GDB.gdb".format(HUC4=HUC4)
        flowlines, joins = extract_flowlines(gdb, target_crs=CRS)

        flowlines.FType = flowlines.FType.astype("uint16")
        flowlines.StreamOrde = flowlines.StreamOrde.astype("uint8")

        print("Extract Done in {:.2f}".format(time() - start))

        print("Serializing flowlines geo file")
        serialize_gdf(flowlines, out_dir / HUC4 / "flowline.feather", index=False)

        print("Writing segment connections")
        serialize_df(joins, out_dir / HUC4 / "flowline_joins.feather", index=False)

        # If a shapefile is needed - need to convert ID back to a float
        # geo_df = flowlines.copy()
        # geo_df.NHDPlusID = geo_df.NHDPlusID.astype("float64")
        # to_shp(geo_df, out_dir / HUC4 / "flowline.shp")

        print("Serializing Done in {:.2f}".format(time() - start))

        print("Done in {:.2f}\n============================".format(time() - start))
