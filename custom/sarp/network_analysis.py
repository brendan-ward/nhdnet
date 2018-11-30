"""This is the main processing script.

Run this after preparing NHD data for the HUC4 identified below.

"""
import os
from time import time

import geopandas as gp
import pandas as pd
from shapely.geometry import MultiLineString

from nhdnet.geometry.points import create_points
from nhdnet.geometry.lines import snap_to_line

from nhdnet.nhd.cut import cut_flowlines
from nhdnet.nhd.network import generate_network, calculate_network_stats
from nhdnet.io import (
    deserialize_df,
    deserialize_gdf,
    to_shp,
    serialize_df,
    serialize_gdf,
)


SNAP_TOLERANCE = 100  # meters - tolerance for waterfalls

HUC2 = "06"
src_dir = "/Users/bcward/projects/data/sarp"
working_dir = "{0}/nhd/{1}".format(src_dir, HUC2)
os.chdir(working_dir)

start = time()

##################### Read NHD data #################
print("------------------- Reading Flowlines -----------")
print("reading flowlines")
flowlines = deserialize_gdf("flowline.feather").set_index("lineID", drop=False)
print("read {} flowlines".format(len(flowlines)))

print("------------------- Preparing barriers ----------")
if os.path.exists("barriers.feather"):
    print("Reading barriers")
    barriers = deserialize_gdf("barriers.feather").set_index("joinID", drop=False)

else:
    ##################### Read dams and waterfalls, and merge #################
    barrier_start = time()

    ##################### Read dams #################
    print("Reading dams")
    dams = deserialize_gdf(
        "{0}/nhd/{1}/dams_post_qa.feather".format(src_dir, HUC2[:2])
    )[["AnalysisID", "geometry"]]
    dams["joinID"] = dams.AnalysisID

    print("Selected {0} dams in region {1}".format(len(dams), HUC2))

    # Snap to lines, assign NHDPlusID and lineID to the point, and drop any that didn't get snapped
    snapper = snap_to_line(flowlines, SNAP_TOLERANCE, prefer_endpoint=False)
    print("Snapping dams")
    snapped = gp.GeoDataFrame(dams.geometry.apply(snapper), crs=flowlines.crs)
    dams = dams.drop(columns=["geometry"]).join(snapped)
    dams = dams.loc[~dams.geometry.isnull()].copy()
    print("{} dams were successfully snapped".format(len(dams)))

    serialize_gdf(dams, "/tmp/dams.feather", index=False)
    # dams.to_csv("snapped_dams.csv", index=False)
    # dams.to_file("snapped_dams.shp", driver="ESRI Shapefile")

    ##################### Read waterfalls #################
    print("Reading waterfalls")
    wf = gp.read_file(
        "{}/Waterfalls_USGS_2017.gdb".format(src_dir), layer="Waterfalls_USGS_2018"
    ).to_crs(flowlines.crs)[["OBJECTID", "HUC_8", "geometry"]]
    wf["joinID"] = wf.OBJECTID.astype("str")
    wf["AnalysisID"] = ""

    # Extract out waterfalls in this HUC
    wf["HUC2"] = wf.HUC_8.str[:2]
    wf = wf.loc[wf.HUC2 == HUC2].copy()
    print("Selected {0} waterfalls in region {1}".format(len(wf), HUC2))

    # Snap to lines, assign NHDPlusID and lineID to the point, and drop any that didn't get snapped
    snapper = snap_to_line(flowlines, SNAP_TOLERANCE, prefer_endpoint=False)
    print("Snapping waterfalls")
    snapped = gp.GeoDataFrame(wf.geometry.apply(snapper), crs=flowlines.crs)
    wf = wf.drop(columns=["geometry"]).join(snapped)
    wf = wf.loc[~wf.geometry.isnull()].copy()
    print("{} waterfalls were successfully snapped".format(len(wf)))

    serialize_gdf(wf, "/tmp/wf.feather", index=False)
    # wf.to_csv("snapped_waterfalls.csv", index=False)
    # wf.to_file("snapped_waterfalls.shp", driver="ESRI Shapefile")

    ##################### Create combined barriers dataset #################
    print("Merging and exporting single barriers file")
    dams["kind"] = "dam"
    wf["kind"] = "waterfall"

    columns = [
        "lineID",
        "NHDPlusID",
        "joinID",
        "AnalysisID",
        "kind",
        "geometry",
        "snap_dist",
        "nearby",
        # "is_endpoint",
    ]

    barriers = dams[columns].append(wf[columns], ignore_index=True, sort=False)
    barriers.set_index("joinID", inplace=True, drop=False)

    print("Checking for duplicates")
    wkt = barriers[["joinID", "geometry"]].copy()
    wkt["point"] = wkt.geometry.apply(lambda g: g.to_wkt())
    barriers = barriers.loc[wkt.drop_duplicates("point").index]
    print("Removed {} duplicate locations".format(len(wkt) - len(barriers)))

    print("serializing barriers")
    # barriers in original but not here are dropped due to likely duplication
    serialize_gdf(barriers, "barriers.feather", index=False)
    tmp = barriers.copy()
    tmp.NHDPlusID = tmp.NHDPlusID.astype("float64")
    to_shp(tmp, "barriers.shp")

    print("Done preparing barriers in {:.2f}s".format(time() - barrier_start))

##################### Cut flowlines #################
cut_start = time()
print("------------------- Cutting flowlines -----------")
print("Starting from {} original segments".format(len(flowlines)))

joins = deserialize_df("flowline_joins.feather")

flowlines, joins, barrier_joins = cut_flowlines(flowlines, barriers, joins)

barrier_joins.upstream_id = barrier_joins.upstream_id.astype("uint32")
barrier_joins.downstream_id = barrier_joins.downstream_id.astype("uint32")

print("Done cutting in {:.2f}".format(time() - cut_start))

print("serializing cut geoms")
serialize_df(joins, "updated_joins.feather", index=False)
serialize_df(barrier_joins, "barrier_joins.feather", index=False)
serialize_gdf(flowlines, "split_flowlines.feather", index=False)

# joins.to_csv("updated_joins.csv", index=False)
# barrier_joins.to_csv("barrier_joins.csv", index=False)
# flowlines.drop(columns=["geometry"]).to_csv("split_flowlines.csv", index=False)
# print("Writing split flowlines shp")
# flowlines.NHDPlusID = flowlines.NHDPlusID.astype("float64")
# flowlines.to_file("split_flowlines.shp", driver="ESRI Shapefile")

print("Done serializing cuts in {:.2f}".format(time() - cut_start))

##################### Create networks #################
print("------------------- Creating networks -----------")
network_start = time()
# remove any origin segments
barrier_segments = list(set(barrier_joins.upstream_id.unique()).difference({0}))

print("generating upstream / downstream indices")
join_ids = joins[["downstream_id", "upstream_id"]]
upstreams = (
    join_ids.groupby("downstream_id")["upstream_id"]
    .unique()
    .reset_index()
    .set_index("downstream_id")
)
downstreams = join_ids.groupby("upstream_id")["downstream_id"].size()

get_upstream_ids = lambda id: upstreams.loc[id].upstream_id
has_multiple_downstreams = lambda id: downstreams.loc[id] > 1

# Create networks from all terminal nodes (no downstream nodes) up to origins or dams (but not including dam segments)
# Note: origins are also those that have a downstream_id but are not the upstream_id of another node
root_ids = joins.loc[
    (joins.downstream_id == 0) | (~joins.downstream_id.isin(joins.upstream_id.unique()))
].upstream_id
print(
    "Starting non-barrier functional network creation for {} origin points".format(
        len(root_ids)
    )
)
for start_id in root_ids:
    network = generate_network(
        start_id,
        get_upstream_ids,
        has_multiple_downstreams,
        stop_segments=set(barrier_segments),
    )

    rows = flowlines.index.isin(network)
    flowlines.loc[rows, "networkID"] = start_id
    flowlines.loc[rows, "networkType"] = "origin_upstream"

    # print("nonbarrier upstream network has {} segments".format(len(network)))

print(
    "Starting barrier functional network creation for {} barriers".format(
        len(barrier_segments)
    )
)
for start_id in barrier_segments:
    network = generate_network(
        start_id,
        get_upstream_ids,
        has_multiple_downstreams,
        stop_segments=set(barrier_segments),
    )

    rows = flowlines.index.isin(network)
    flowlines.loc[rows, "networkID"] = start_id
    flowlines.loc[rows, "networkType"] = "barrier_upstream"
    # print("barrier upstream network has {} segments".format(len(network)))

# drop anything that didn't get assigned a network
# Note: network_df is still indexed on lineID
network_df = flowlines.loc[~flowlines.networkID.isnull()].copy()
network_df.networkID = network_df.networkID.astype("uint32")

serialize_gdf(network_df, "network_segments.feather", index=False)
# network_df.drop(columns=["geometry"]).to_csv("network_segments.csv", index=False)
# network_df.to_file("network_segments.shp", driver="ESRI Shapefile")

print(
    "Created {0} networks in {1:.2f}s".format(
        len(network_df.networkID.unique()), time() - network_start
    )
)


##################### Network stats #################
print("------------------- Aggregating network info -----------")

network_stats = calculate_network_stats(network_df)

network_stats.to_csv(
    "network_stats.csv",
    columns=["km", "miles", "NetworkSinuosity", "NumSizeClassGained"],
    index_label="networkID",
)

network_stats = network_stats[["miles", "NetworkSinuosity", "NumSizeClassGained"]]

# join to upstream networks
barriers = barriers.set_index("joinID")[["kind"]]
barrier_joins.set_index("joinID", inplace=True)
upstream_networks = (
    barriers.join(barrier_joins.upstream_id)
    .join(network_stats, on="upstream_id")
    .rename(columns={"upstream_id": "upNetID", "miles": "UpstreamMiles"})
)

network_by_lineID = network_df[["lineID", "networkID"]].set_index("lineID")
downstream_networks = (
    barrier_joins.join(network_by_lineID, on="downstream_id")
    .join(network_stats, on="networkID")
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

barrier_networks.to_csv("barrier_network.csv", index_label="joinID")

# TODO: if downstream network extends off this HUC, it will be null in the above and AbsoluteGainMin will be wrong

##################### Dissolve networks on networkID ########################
network_ids = network_stats.index

columns = ["networkID", "geometry"] + list(network_stats.columns)
networks = gp.GeoDataFrame(columns=columns, geometry="geometry", crs=flowlines.crs)

network_df = network_df.set_index("networkID", drop=False)

# join in network stats
for id in network_ids:
    stats = network_stats.loc[id]

    geometries = network_df.loc[id].geometry
    # converting to list is very inefficient but otherwise
    # we get an error in shapely internals
    if isinstance(geometries, gp.GeoSeries):
        geometry = MultiLineString(geometries.values.tolist())
    else:
        geometry = MultiLineString([geometries])

    values = [id, geometry] + [stats[c] for c in network_stats.columns]
    networks.loc[id] = gp.GeoSeries(values, index=columns)

# same as network stats
# networks.drop(columns=["geometry"]).to_csv("network.csv", index=False)

print("Writing dissolved network shapefile")
serialize_gdf(networks, "network.feather", index=False)

to_shp(networks, "network.shp")


print("All done in {:.2f}".format(time() - start))

