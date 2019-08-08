"""
Download NHD Plus HR flowline data for every HUC4 in the SARP region.
"""


from pathlib import Path
import os

from constants import REGIONS
from nhdnet.nhd.download import download_huc4
from nhdnet.nhd.legacy.download import download_huc4_mr

nhd_dir = Path("../data/sarp/nhd/raw_nhd_2019")

for HUC2 in REGIONS:
    for i in REGIONS[HUC2]:
        HUC4 = "{0}{1:02d}".format(HUC2, i)
        filename = nhd_dir / "{HUC4}.zip".format(HUC4=HUC4)

        if not os.path.exists(filename):
            # TODO: remove check for region 8
            if HUC2 == "08":
                download_huc4_mr(HUC4, filename)

            else:
                download_huc4(HUC4, filename)
