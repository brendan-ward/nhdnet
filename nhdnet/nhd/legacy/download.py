from shutil import copyfileobj
import requests

### NHD Medium Resolution
# Listing URL: https://prd-tnm.s3.amazonaws.com/index.html?prefix=StagedProducts/Hydrography/NHD/HU4/HighResolution/GDB/
DATA_URL = "https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/NHD/HU4/HighResolution/GDB/NHD_H_{HUC4}_HU4_GDB.zip"


def download_huc4_mr(HUC4, filename):
    """Download HUC4 geodatabase (flowlines and boundaries) from NHD Medium resolution data distribution site
    
    Parameters
    ----------
    HUC4 : str
        HUC4 ID code
    filename : str
        output filename.  Will always overwrite this filename.
    """

    with requests.get(DATA_URL.format(HUC4=HUC4), stream=True) as r:
        with open(filename, "wb") as out:
            print(
                "Downloading HUC4: {HUC4} ({size:.2f} MB)".format(
                    HUC4=HUC4, size=int(r.headers["Content-Length"]) / 1024 ** 2
                )
            )

            # Use a streaming copy to download the bytes of this file
            copyfileobj(r.raw, out)
