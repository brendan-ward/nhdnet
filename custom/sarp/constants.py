"""Constants used in other scripts."""


# Mapping of region to HUC4 IDs that are present within the SARP boundary
REGIONS = {
    "02": [7, 8],
    "03": list(range(1, 19)),
    "05": [5, 7, 9, 10, 11, 13, 14],
    "06": list(range(1, 5)),
    "07": [10, 11, 14],
    # 08 is a special case; it is medium resolution until beta high resolution version is posted
    # "08": list(range(1, 10)),
    "10": [24, 28, 29, 30],
    "11": [1, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14],
    "12": list(range(1, 12)),
    "13": [3, 4, 5, 7, 8, 9],
}

# Group regions based on which ones flow into each other
# Note: many of these flow into region 08, which is not yet available
# The total size of the region group needs to be limited based on available memory and the size of the output shapefiles
# from the network analysis, which cannot exceed 2 GB.
REGION_GROUPS = {
    "02": ["02"],
    "03": ["03"],
    "05_06": ["05", "06"],
    "07_10": ["07", "10"],
    # 08 is a special case; it is medium resolution until beta high resolution version is posted
    # "08": ["08"],
    "11": ["11"],
    "12": ["12"],
    "13": ["13"],
}

# By default, barriers are snapped automatically only if within this distance
SNAP_TOLERANCE = 100  # meters

# All barriers that are within 10 meters of each other are reduced to the first one
# Note: Dams within 30 meters of each other are considered duplicates
DUPLICATE_TOLERANCE = 10  # meters

# Use USGS CONUS Albers (EPSG:102003): https://epsg.io/102003    (same as other SARP datasets)
# use Proj4 syntax, since GeoPandas doesn't properly recognize it's EPSG Code.
# CRS = "+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=37.5 +lon_0=-96 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs"
CRS = {
    "proj": "aea",
    "lat_1": 29.5,
    "lat_2": 45.5,
    "lat_0": 37.5,
    "lon_0": -96,
    "x_0": 0,
    "y_0": 0,
    "datum": "NAD83",
    "units": "m",
    "no_defs": True,
}

# The columns to include from the barriers data for use in the network analysis.
# The same columns are included for each type of barrier.
BARRIER_COLUMNS = [
    "lineID",
    "NHDPlusID",
    "joinID",
    "geometry",
    "snap_dist",
    "nearby",
    "kind",
]

# Used to filter small barriers by Potential_Project (small barriers)
# based on guidance from Kat
KEEP_POTENTIAL_PROJECT = [
    "Severe Barrier",
    "Moderate Barrier",
    "Inaccessible",
    "Significant Barrier",
    "No Upstream Channel",
    "Indeterminate",
    "Potential Project",
    "Proposed Project",
]

# Used to filter Potential_Project (small barriers)
# These are DROPPED from all analysis and mapping
# TODO: add "No Upstream Habitat", "No Upstream Channel" ?
DROP_POTENTIAL_PROJECT = ["No", "No Barrier", "No Crossing", "Past Project"]


# Used to filter small barriers and dams by SNAP2018, based on guidance from Kat
# Note: dropped barriers are still shown on the map, but not included in the network analysis
# Note: 0 value indicates N/A
DROP_SNAP2018 = [6, 8]

# These are excluded from network analysis / prioritization, but included for mapping
EXCLUDE_SNAP2018 = [5, 7, 9, 10]

# Used to filter dams by PotentialFeasibility
# based on guidance from Kat
DROP_FEASIBILITY = [7, 19]

# These are excluded from network analysis / prioritization, but included for mapping
EXCLUDE_FEASIBILITY = [16]
