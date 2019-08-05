from shutil import copyfileobj
import requests

DATA_URL = "https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/NHDPlusHR/Beta/GDB/NHDPLUS_H_{HUC4}_HU4_GDB.zip"


def download_huc4(HUC4, filename):
    with requests.get(DATA_URL.format(HUC4=HUC4), stream=True) as r:
        with open(filename, "wb") as out:
            print(
                "Downloading HUC4: {HUC4} ({size:.2f} MB)".format(
                    HUC4=HUC4, size=int(r.headers["Content-Length"]) / 1024 ** 2
                )
            )

            # Use a streaming copy to download the bytes of this file
            copyfileobj(r.raw, out)
