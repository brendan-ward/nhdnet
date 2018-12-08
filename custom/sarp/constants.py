REGIONS = {
    "02": [7, 8],
    "03": list(range(1, 19)),
    "05": [5, 7, 9, 10, 11, 13, 14],
    "06": list(range(1, 5)),
    "07": [10, 11, 14],
    "10": [24, 28, 29, 30],
    "11": [1, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14],
    "12": list(range(1, 12)),
    "13": [3, 4, 5, 7, 8, 9],
}

SNAP_TOLERANCE = 100  # meters


# Use USGS CONUS Albers (EPSG:102003): https://epsg.io/102003    (same as other SARP datasets)
# use Proj4 syntax, since GeoPandas doesn't properly recognize it's EPSG Code.
CRS = "+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=37.5 +lon_0=-96 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs"

BARRIER_COLUMNS = [
    "lineID",
    "NHDPlusID",
    "joinID",
    "AnalysisID",
    "geometry",
    "snap_dist",
    "nearby",
    "kind",
]
