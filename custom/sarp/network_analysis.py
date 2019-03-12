"""This is the main processing script.

Run this after preparing NHD data for the region group identified below.

"""
import os
from time import time

import geopandas as gp
import pandas as pd
import numpy as np
from shapely.geometry import MultiLineString

from nhdnet.nhd.cut import cut_flowlines
from nhdnet.nhd.network import generate_networks
from nhdnet.io import (
    deserialize_df,
    deserialize_gdf,
    to_shp,
    serialize_df,
    serialize_gdf,
)

from constants import BARRIER_COLUMNS, REGION_GROUPS
from stats import calculate_network_stats


### START Runtime variables
# These should be the only variables that need to be changed at runtime

RESUME = False
# Set to True to re-use previously calculated intermediate files.  This can save time when debugging later steps.

SMALL_BARRIERS = True
# Set to True when you want to include small barriers in the analysis.  When included, small barriers are treated as HARD barriers. and WILL affect the results for dams.

group = "07_10"  # Identifier of region group or region

### END Runtime variables


src_dir = "/Users/bcward/projects/data/sarp"
working_dir = "{0}/nhd/region/{1}".format(src_dir, group)
os.chdir(working_dir)


# INPUT files from merge.py
flowline_feather = "flowline.feather"
joins_feather = "flowline_joins.feather"

# INPUT files from prepare_floodplain_stats.py
fp_feather = "{}/floodplain_stats.feather".format(src_dir)

# INPUT files from prepare_dams.py, prepare_waterfalls.py, prepare_small_barriers.py
dams_feather = "{}/snapped_dams.feather".format(src_dir)
waterfalls_feather = "{}/snapped_waterfalls.feather".format(src_dir)
small_barriers_feather = "{}/snapped_small_barriers.feather".format(src_dir)


suffix = "small_barriers" if SMALL_BARRIERS else "dams"

# INTERMEDIATE files
barrier_feather = "barriers_{}.feather".format(suffix)
split_flowline_feather = "split_flowlines_{}.feather".format(suffix)
updated_joins_feather = "updated_joins_{}.feather".format(suffix)
barrier_joins_feather = "barrier_joins_{}.feather".format(suffix)
network_feather = "network_{}.feather".format(suffix)
network_segments_feather = "network_segments_{}.feather".format(suffix)

# OUTPUT files
network_stats_csv = "network_stats_{}.csv".format(suffix)
barrier_network_csv = "barriers_network_{}.csv".format(suffix)


start = time()

##################### Read NHD data #################
print("------------------- Reading Flowlines -----------")
print("reading flowlines")
flowlines = deserialize_gdf(flowline_feather).set_index("lineID", drop=False)
print("read {} flowlines".format(len(flowlines)))

print("------------------- Preparing barriers ----------")
barrier_start = time()
dams = deserialize_gdf(dams_feather)
dams["kind"] = "dam"

# TODO: will be handled in prepare_dams.py
dams["HUC2"] = dams.HUC4.str[:2]

dams = dams.loc[dams.HUC2.isin(REGION_GROUPS[group])].copy()
print("Selected {} dams".format(len(dams)))

wf = deserialize_gdf(waterfalls_feather)
wf["kind"] = "waterfall"
wf["AnalysisID"] = ""
wf = wf.loc[wf.HUC2.isin(REGION_GROUPS[group])].copy()
print("Selected {} waterfalls".format(len(wf)))

barriers = dams[BARRIER_COLUMNS].append(
    wf[BARRIER_COLUMNS], ignore_index=True, sort=False
)

if SMALL_BARRIERS:
    sb = deserialize_gdf(small_barriers_feather)
    sb["kind"] = "small_barrier"
    sb = sb.loc[sb.HUC2.isin(REGION_GROUPS[group])].copy()
    print("Selected {} small barriers".format(len(sb)))

    if len(sb):
        barriers = barriers.append(sb[BARRIER_COLUMNS], ignore_index=True, sort=False)

barriers.set_index("joinID", inplace=True, drop=False)

print("Checking for duplicates")
wkt = barriers[["joinID", "geometry"]].copy()
wkt["point"] = wkt.geometry.apply(lambda g: g.to_wkt())
barriers = barriers.loc[wkt.drop_duplicates("point").index]
print("Removed {} duplicate locations".format(len(wkt) - len(barriers)))

print("serializing barriers")
# barriers in original but not here are dropped due to likely duplication
serialize_gdf(barriers, barrier_feather, index=False)
tmp = barriers.copy()
tmp.NHDPlusID = tmp.NHDPlusID.astype("float64")
to_shp(tmp, barrier_feather.replace(".feather", ".shp"))

print("Done preparing barriers in {:.2f}s".format(time() - barrier_start))


##################### Cut flowlines #################
if RESUME and os.path.exists(split_flowline_feather):
    print("reading cut segments and joins")

    flowlines = deserialize_gdf(split_flowline_feather).set_index("lineID", drop=False)
    joins = deserialize_df(updated_joins_feather)
    barrier_joins = deserialize_df(barrier_joins_feather).set_index(
        "joinID", drop=False
    )

else:
    cut_start = time()
    print("------------------- Cutting flowlines -----------")
    print("Starting from {} original segments".format(len(flowlines)))

    joins = deserialize_df(joins_feather)
    # since all other lineIDs use HUC4 prefixes, this should be unique
    # Use the first HUC2 for the region group
    next_segment_id = int(REGION_GROUPS[group][0]) * 1000000 + 1
    flowlines, joins, barrier_joins = cut_flowlines(
        flowlines, barriers, joins, next_segment_id=next_segment_id
    )
    print("Done cutting in {:.2f}".format(time() - cut_start))

    print("serializing cut geoms")

    barrier_joins.upstream_id = barrier_joins.upstream_id.astype("uint32")
    barrier_joins.downstream_id = barrier_joins.downstream_id.astype("uint32")
    barrier_joins.set_index("joinID", drop=False)
    serialize_df(joins, updated_joins_feather, index=False)
    serialize_df(barrier_joins, barrier_joins_feather, index=False)
    serialize_gdf(flowlines, split_flowline_feather, index=False)

    print("Done serializing cuts in {:.2f}".format(time() - cut_start))


##################### Create networks #################
print("------------------- Creating networks -----------")
network_start = time()

# remove any origin segments
barrier_segments = barrier_joins.loc[barrier_joins.upstream_id != 0][["upstream_id"]]

print("generating upstream index")
# Remove origins, terminals, and barrier segments
upstreams = (
    joins.loc[
        (joins.upstream_id != 0)
        & (joins.downstream_id != 0)
        & (~joins.upstream_id.isin(barrier_segments.upstream_id))
    ]
    .groupby("downstream_id")["upstream_id"]
    .apply(list)
    .to_dict()
)

# Create networks from all terminal nodes (no downstream nodes) up to barriers
# Note: origins are also those that have a downstream_id but are not the upstream_id of another node
origin_idx = (joins.downstream_id == 0) | (
    ~joins.downstream_id.isin(joins.upstream_id.unique())
)
not_barrier_idx = ~joins.upstream_id.isin(barrier_segments.upstream_id)
root_ids = joins.loc[origin_idx & not_barrier_idx][["upstream_id"]].copy()

print(
    "Starting network creation for {} origin points and {} barriers".format(
        len(root_ids), len(barrier_segments)
    )
)

origin_network_segments = generate_networks(root_ids, upstreams)
origin_network_segments["type"] = "origin"

barrier_network_segments = generate_networks(barrier_segments, upstreams)
barrier_network_segments["type"] = "barrier"

# Append and join back to flowlines, dropping anything that didn't get networks
network_df = origin_network_segments.append(barrier_network_segments, sort=False)
network_df = flowlines.join(network_df, how="inner")

print(
    "{0} networks done in {1:.2f}".format(
        len(network_df.networkID.unique()), time() - network_start
    )
)

serialize_gdf(network_df, network_segments_feather, index=False)


##################### Network stats #################
print("------------------- Aggregating network info -----------")

# Read in associated floodplain info and join
fp_stats = deserialize_df(fp_feather)
fp_stats = fp_stats.loc[fp_stats.HUC2.isin(REGION_GROUPS[group])].set_index("NHDPlusID")

network_df = network_df.join(fp_stats, on="NHDPlusID")

print("calculating network stats")
stats_start = time()
network_stats = calculate_network_stats(network_df)
print("done calculating network stats in {0:.2f}".format(time() - stats_start))

network_stats.to_csv(network_stats_csv, index_label="networkID")

# Drop columns we don't need later
network_stats = network_stats[
    ["miles", "NetworkSinuosity", "NumSizeClassGained", "PctNatFloodplain"]
]


print("calculating upstream and downstream networks for barriers")
# join to upstream networks
barriers = barriers.set_index("joinID")[["kind"]]
barrier_joins.set_index("joinID", inplace=True)
upstream_networks = (
    barriers.join(barrier_joins.upstream_id)
    .join(network_stats, on="upstream_id")
    .fillna(0)
    .rename(columns={"upstream_id": "upNetID", "miles": "UpstreamMiles"})
)

network_by_lineID = network_df[["lineID", "networkID"]].set_index("lineID")
downstream_networks = (
    barrier_joins.join(network_by_lineID, on="downstream_id")
    .join(network_stats, on="networkID")
    .fillna(0)
    .rename(columns={"networkID": "downNetID", "miles": "DownstreamMiles"})[
        ["downNetID", "DownstreamMiles"]
    ]
)


barrier_networks = upstream_networks.join(downstream_networks)

# Absolute gain is minimum of upstream or downstream miles
barrier_networks["AbsoluteGainMi"] = barrier_networks[
    ["UpstreamMiles", "DownstreamMiles"]
].min(axis=1)
barrier_networks.upNetID = barrier_networks.upNetID.fillna(0).astype("uint32")
barrier_networks.downNetID = barrier_networks.downNetID.fillna(0).astype("uint32")
barrier_networks.NumSizeClassGained = barrier_networks.NumSizeClassGained.fillna(
    0
).astype("uint8")

barrier_networks.to_csv(barrier_network_csv, index_label="joinID")


# TODO: if downstream network extends off this HUC, it will be null in the above and AbsoluteGainMin will be wrong

##################### Dissolve networks on networkID ########################
print("Dissolving networks")
dissolve_start = time()

network_df = network_df.set_index("networkID", drop=False)

dissolved = (
    network_df[["geometry"]]
    .groupby(network_df.index)
    .geometry.apply(list)
    .apply(MultiLineString)
)

networks = gp.GeoDataFrame(network_stats.join(dissolved), crs=flowlines.crs)

# add networkID back
networks["networkID"] = networks.index.values.astype("uint32")

print("Network dissolve done in {0:.2f}".format(time() - dissolve_start))

print("Writing dissolved network shapefile")
serialize_gdf(networks, network_feather, index=False)
to_shp(networks, network_feather.replace(".feather", ".shp"))

print("All done in {:.2f}".format(time() - start))

