import pandas as pd
import geopandas as gp
import numpy as np
from time import time

from nhdnet.geometry.lines import (
    cut_line_at_points,
    cut_line_at_point,
    calculate_sinuosity,
)

# if points are within this distance of start or end coordinate, nothing is cut
# EPS = 1e-6

# points within 1 meter of the end are close enough not to cut, and instead get assigned to the endpoints
EPS = 1


def update_joins(joins, new_downstreams, new_upstreams):
    """
    Update new upstream and downstream segment IDs into joins table.

    Parameters
    ----------
    joins : DataFrame
        contains records with upstream_id and downstream_id representing joins between segments
    new_dowstreams : Series
        Series, indexed on original line ID, with the new downstream ID for each original line ID
    new_upstreams : Series
        Series, indexed on original line ID, with the new upstream ID for each original line ID
    
    Returns
    -------
    DataFrame
    """

    joins = joins.join(new_downstreams, on="downstream_id").join(
        new_upstreams, on="upstream_id"
    )

    # copy new downstream IDs across
    idx = joins.new_downstream_id.notnull()
    joins.loc[idx, "downstream_id"] = joins[idx].new_downstream_id.astype("uint32")

    # copy new upstream IDs across
    idx = joins.new_upstream_id.notnull()
    joins.loc[idx, "upstream_id"] = joins[idx].new_upstream_id.astype("uint32")

    return joins.drop(columns=["new_downstream_id", "new_upstream_id"])


def cut_flowlines(flowlines, barriers, joins, next_segment_id=None):
    print("Starting number of segments: {:,}".format(len(flowlines)))
    print("Cutting in {:,} barriers".format(len(barriers)))

    # Our segment ids are ints, so just increment from the last one we had from NHD
    if next_segment_id is None:
        next_segment_id = int(flowlines.index.max() + 1)

    # origLineID is the original lineID of the segment that was split
    # can be used to join back to flowlines to get the other props
    # columns = ["length", "sinuosity", "geometry", "lineID", "origLineID"]

    # join barriers on original lineID to joins between lines
    # TODO: this isn't right
    # barrier_joins = (
    #     barriers[["geometry", "barrierID", "lineID"]]
    #     .join(joins.set_index("upstream_id")["downstream_id"], on="lineID")
    #     .join(joins.set_index("downstream_id")["upstream_id"], on="lineID")
    # )

    # Initially, the upstream and downstream IDs for each barrier are the segment it is on
    barrier_joins = barriers[["barrierID", "lineID"]].copy()
    barrier_joins["upstream_id"] = barrier_joins.lineID
    barrier_joins["downstream_id"] = barrier_joins.lineID

    # join barriers to lines and extract those that have segments (via inner join)
    barrier_segments = flowlines.join(
        barriers[["geometry", "barrierID", "lineID"]].set_index("lineID"),
        rsuffix="_barrier",
        how="inner",
    )

    # extract flowlines that don't have barriers
    no_barrier_segments = flowlines.loc[~flowlines.index.isin(barrier_segments.index)]

    # add in count of barriers per segment
    barrier_segments = barrier_segments.join(
        barrier_segments.groupby(level=0).size().rename("barriers")
    )

    # calculate the position of each barrier on each segment
    barrier_segments["linepos"] = barrier_segments.apply(
        lambda row: row.geometry.project(row.geometry_barrier), axis=1
    )

    # ordinate the barriers by their projected distance on the line
    # Order this so we are always moving from upstream end to downstream end
    # TODO: only need to sort the splits, not the ones at the terminals
    barrier_segments = barrier_segments.rename_axis("idx").sort_values(
        by=["idx", "linepos"], ascending=True
    )

    # Barriers are on upstream or downstream end of segment if they are within
    # EPS of the ends.  Otherwise, they are splits
    barrier_segments["on_upstream"] = barrier_segments.linepos <= EPS
    barrier_segments["on_downstream"] = (
        barrier_segments.linepos >= barrier_segments.length - EPS
    )

    # Otherwise they are splits
    split_segments = barrier_segments.loc[
        ~(barrier_segments.on_upstream | barrier_segments.on_downstream)
    ]

    # for those barriers on upstream end, their downstream_id is the lineID they are on
    idx = barrier_joins.index.isin(
        barrier_segments.loc[barrier_segments.on_upstream].barrierID
    )
    barrier_joins.loc[idx, "downstream_id"] = barrier_joins.loc[idx].lineID

    # for those barriers on the downstream end, their upstream_id is the lineID they are on
    idx = barrier_joins.index.isin(
        barrier_segments.loc[barrier_segments.on_downstream].barrierID
    )
    barrier_joins.loc[idx, "upstream_id"] = barrier_joins.loc[idx].lineID

    # if there is only 1 barrier per segment, things are much easier!

    #### WIP: Easy splints - 1 barrier per segment
    easy_splits = split_segments.loc[split_segments.barriers == 1].reset_index()

    geoms = (
        easy_splits.apply(
            lambda row: cut_line_at_point(row.geometry, row.geometry_barrier), axis=1
        )
        .apply(pd.Series)
        .rename(columns={0: "upstream", 1: "downstream"})
    )

    easy_splits = (
        easy_splits[["lineID", "barrierID", "NHDPlusID"]]
        .join(geoms)
        .rename(columns={"lineID": "origLineID"})
    )

    new_segments = gp.GeoDataFrame(
        easy_splits.melt(
            id_vars=["origLineID", "barrierID", "NHDPlusID"],
            value_vars=["upstream", "downstream"],
            value_name="geometry",
        )
        .sort_values(by="origLineID")
        .rename(columns={"variable": "side"})
        .reset_index(drop=True)
    )
    # generate a new ID for each segment
    new_segments["lineID"] = next_segment_id + new_segments.index

    # join in data from flowlines
    new_flowlines = new_segments.join(
        flowlines[["sizeclass", "streamorder"]], on="origLineID"
    )

    # calculate length and sinuosity
    new_flowlines["length"] = new_flowlines.length
    new_flowlines["sinuosity"] = new_flowlines.geometry.apply(calculate_sinuosity)

    # Append new flowlines
    updated_flowlines = no_barrier_segments.append(
        new_flowlines[
            [
                "lineID",
                "NHDPlusID",
                "sizeclass",
                "streamorder",
                "length",
                "sinuosity",
                "geometry",
            ]
        ],
        ignore_index=True,
        sort=False,
    ).set_index("lineID", drop=False)

    # update joins
    # NOTE: these are from the perspective of the line segment that was split
    new_downstreams = (
        new_segments.loc[(new_segments.side == "upstream")]
        .set_index("origLineID")
        .lineID.rename("new_downstream_id")
    )
    new_upstreams = (
        new_segments.loc[(new_segments.side == "downstream")]
        .set_index("origLineID")
        .lineID.rename("new_upstream_id")
    )

    updated_joins = update_joins(joins, new_downstreams, new_upstreams)

    # Add in new joins for the splits
    new_joins = new_segments[["NHDPlusID", "lineID", "barrierID", "side"]].pivot(
        index="barrierID", columns="side"
    )
    new_joins.columns = ["downstream", "upstream", "downstream_id", "upstream_id"]
    new_joins["type"] = "internal"

    updated_joins = updated_joins.append(new_joins, ignore_index=True, sort=False)

    # update barrier joins
    # NOTE: these are from the barrier that split the segment
    new_upstreams = (
        new_segments.loc[(new_segments.side == "upstream")]
        .set_index("origLineID")
        .lineID.rename("new_upstream_id")
    )
    new_downstreams = (
        new_segments.loc[(new_segments.side == "downstream")]
        .set_index("origLineID")
        .lineID.rename("new_downstream_id")
    )
    barrier_joins = update_joins(barrier_joins, new_downstreams, new_upstreams)

    # TODO: hard splits

    return updated_flowlines, updated_joins, barrier_joins


# original method below

# def cut_flowlines(flowlines, barriers, joins, next_segment_id=None):
#     start = time()
#     print("Starting number of segments: {:,}".format(len(flowlines)))
#     print("Cutting in {:,} barriers".format(len(barriers)))

#     # Our segment ids are ints, so just increment from the last one we had from NHD
#     if next_segment_id is None:
#         next_segment_id = int(flowlines.index.max() + 1)

#     # origLineID is the original lineID of the segment that was split
#     # can be used to join back to flowlines to get the other props
#     columns = ["length", "sinuosity", "geometry", "lineID", "origLineID"]

#     updated_upstreams = dict()
#     updated_downstreams = dict()

#     barrier_joins = []
#     new_segments = []
#     new_joins = []

#     segments_with_barriers = flowlines.loc[flowlines.index.isin(barriers.lineID)][
#         ["NHDPlusID", "geometry"]
#     ]
#     print("{:,} segments have at least one barrier".format(len(segments_with_barriers)))
#     for row in segments_with_barriers.itertuples():
#         idx = row.Index
#         # print("-----------------------\n\nlineID", idx)

#         # Find upstream and downstream segments
#         upstreams = joins.loc[joins.downstream_id == idx]
#         downstreams = joins.loc[joins.upstream_id == idx]

#         # Barriers on this line
#         points = barriers.loc[barriers.lineID == idx].copy()

#         # ordinate the barriers by their projected distance on the line
#         # Order this so we are always moving from upstream end to downstream end
#         line = row.geometry
#         length = line.length
#         points["linepos"] = points.geometry.apply(lambda p: line.project(p))
#         points.sort_values("linepos", inplace=True, ascending=True)

#         # by definition, splits must occur after the first coordinate in the line and before the last coordinate
#         split_points = points.loc[
#             (points.linepos > EPS) & (points.linepos < (length - EPS))
#         ]

#         segments = []
#         if len(split_points):
#             # print(length, split_points.linepos)
#             lines = cut_line_at_points(line, split_points.geometry)
#             num_segments = len(lines)
#             ids = list(range(next_segment_id, next_segment_id + num_segments))
#             next_segment_id += num_segments

#             # make id, segment pairs
#             segments = list(zip(ids, lines))

#             # add these to flowlines
#             for i, (id, segment) in enumerate(segments):
#                 values = gp.GeoSeries(
#                     [segment.length, calculate_sinuosity(segment), segment, id, idx],
#                     index=columns,
#                 )
#                 new_segments.append(values)

#                 if i < num_segments - 1:
#                     # add a join for this barrier
#                     barrier_joins.append(
#                         [row.NHDPlusID, split_points.iloc[i].joinID, id, ids[i + 1]]
#                     )

#             # Since are UPDATING the downstream IDs of the upstream segments, we build a mapping of the original
#             # downstream_id to the updated value for the new segment.
#             # sets downstream_id
#             updated_downstreams.update({id: ids[0] for id in upstreams.downstream_id})
#             # sets upstream_id
#             updated_upstreams.update({id: ids[-1] for id in downstreams.upstream_id})

#             # # add joins for everything after first node
#             new_joins.extend(
#                 [[ids[i], id, np.nan, np.nan] for i, id in enumerate(ids[1:])]
#             )

#         # If barriers are at the downstream-most point or upstream-most point
#         us_points = points.loc[points.linepos <= EPS]
#         ds_points = points.loc[points.linepos >= (length - EPS)]

#         # Handle any points on the upstream or downstream end of line
#         if len(us_points):
#             # Create a record for each that has downstream set to the first segment if any, or
#             # NHDPlusID of this segment
#             # Do this for every upstream segment (if there are multiple upstream nodes)
#             downstream_id = segments[0][0] if segments else idx
#             for barrier in us_points.itertuples():
#                 for upstream in upstreams.itertuples():
#                     barrier_joins.append(
#                         [
#                             row.NHDPlusID,
#                             barrier.joinID,
#                             upstream.upstream_id,
#                             downstream_id,
#                         ]
#                     )

#         if len(ds_points):
#             # Create a record for each that has upstream set to the last segment if any, or
#             # NHDPlusID of this segment
#             # Do this for every downstream segment (if there are multiple downstream nodes)
#             upstream_id = segments[-1][0] if segments else idx
#             for barrier in ds_points.itertuples():
#                 for downstream in downstreams.itertuples():
#                     barrier_joins.append(
#                         [
#                             row.NHDPlusID,
#                             barrier.joinID,
#                             upstream_id,
#                             downstream.downstream_id,
#                         ]
#                     )

#     print("{:.2f} to cut barriers".format(time() - start))

#     new_flowlines = gp.GeoDataFrame(
#         new_segments, columns=columns, crs=flowlines.crs, geometry="geometry"
#     )
#     new_flowlines = new_flowlines.join(
#         flowlines.drop(columns=["length", "sinuosity", "geometry", "lineID"]),
#         on="origLineID",
#     )

#     # Drop all segments replaced by new segments
#     # Join to new flowlines after reseting index to make append easier
#     # Then add the index back
#     flowlines = flowlines.loc[~flowlines.index.isin(new_flowlines.origLineID)].append(
#         new_flowlines.drop(columns=["origLineID"]), sort=False, ignore_index=True
#     )
#     # reset types
#     flowlines.lineID = flowlines.lineID.astype("uint32")
#     flowlines.set_index("lineID", drop=False, inplace=True)

#     print("{:.2f} to append flowlines".format(time() - start))

#     print("{:,} segments after cutting in barriers".format(len(flowlines)))

#     ### Append in new joins
#     joins = joins.append(
#         pd.DataFrame(
#             new_joins,
#             columns=["upstream_id", "downstream_id", "upstream", "downstream"],
#         ),
#         ignore_index=True,
#         sort=False,
#     )

#     # update joins that had new segments inserted between them
#     index = joins.upstream_id.isin(updated_upstreams.keys())
#     joins.loc[index, "upstream_id"] = joins.loc[index].upstream_id.map(
#         updated_upstreams
#     )

#     index = joins.downstream_id.isin(updated_downstreams.keys())
#     joins.loc[index, "downstream_id"] = joins.loc[index].downstream_id.map(
#         updated_downstreams
#     )

#     joins.upstream_id = joins.upstream_id.astype("uint32")
#     joins.downstream_id = joins.downstream_id.astype("uint32")

#     ### Setup data frame for barrier joins
#     barrier_joins = pd.DataFrame(
#         barrier_joins, columns=["NHDPlusID", "joinID", "upstream_id", "downstream_id"]
#     )

#     # Update barrier joins based on upstreams or downstreams that were updated with
#     # new segment ids
#     index = barrier_joins.upstream_id.isin(updated_upstreams.keys())
#     barrier_joins.loc[index, "upstream_id"] = barrier_joins.loc[index].upstream_id.map(
#         updated_upstreams
#     )

#     index = barrier_joins.downstream_id.isin(updated_downstreams.keys())
#     barrier_joins.loc[index, "downstream_id"] = barrier_joins.loc[
#         index
#     ].downstream_id.map(updated_downstreams)

#     barrier_joins.upstream_id = barrier_joins.upstream_id.astype("uint32")
#     barrier_joins.downstream_id = barrier_joins.downstream_id.astype("uint32")

#     return flowlines, joins, barrier_joins.set_index("joinID", drop=False)
