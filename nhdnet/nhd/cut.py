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


def prep_new_flowlines(flowlines, new_segments):
    # join in data from flowlines into new segments
    new_flowlines = new_segments.join(
        flowlines[["sizeclass", "streamorder"]], on="origLineID"
    )

    # calculate length and sinuosity
    new_flowlines["length"] = new_flowlines.length
    new_flowlines["sinuosity"] = new_flowlines.geometry.apply(calculate_sinuosity)

    return new_flowlines[
        [
            "lineID",
            "NHDPlusID",
            "sizeclass",
            "streamorder",
            "length",
            "sinuosity",
            "geometry",
        ]
    ]


def cut_flowlines(flowlines, barriers, joins, next_segment_id=None):
    print("Starting number of segments: {:,}".format(len(flowlines)))
    print("Cutting in {:,} barriers".format(len(barriers)))

    # Our segment ids are ints, so just increment from the last one we had from NHD
    if next_segment_id is None:
        next_segment_id = int(flowlines.index.max() + 1)

    # Initially, the upstream and downstream IDs for each barrier are the segment it is on
    barrier_joins = barriers[["barrierID", "lineID"]].copy()
    barrier_joins["upstream_id"] = barrier_joins.lineID
    barrier_joins["downstream_id"] = barrier_joins.lineID

    # join barriers to lines and extract those that have segments (via inner join)
    barrier_segments = flowlines[["lineID", "NHDPlusID", "geometry"]].join(
        barriers[["geometry", "barrierID", "lineID"]].set_index("lineID"),
        rsuffix="_barrier",
        how="inner",
    )

    # extract flowlines that don't have barriers
    no_barrier_segments = flowlines.loc[~flowlines.index.isin(barrier_segments.index)]

    # add in count of barriers per segment
    # barrier_segments = barrier_segments.join(
    #     barrier_segments.groupby(level=0).size().rename("barriers")
    # )

    # calculate the position of each barrier on each segment
    barrier_segments["linepos"] = barrier_segments.apply(
        lambda row: row.geometry.project(row.geometry_barrier), axis=1
    )

    # ordinate the barriers by their projected distance on the line
    # Order this so we are always moving from upstream end to downstream end
    # TODO: only need to sort the splits, not the ones at the terminals
    # barrier_segments = barrier_segments.rename_axis("idx").sort_values(
    #     by=["idx", "linepos"], ascending=True
    # )

    # Barriers are on upstream or downstream end of segment if they are within
    # EPS of the ends.  Otherwise, they are splits

    ### Upstream endpoint barriers
    barrier_segments["on_upstream"] = barrier_segments.linepos <= EPS
    print(
        "{:,} barriers on upstream point of their segments".format(
            len(barrier_segments.loc[barrier_segments.on_upstream])
        )
    )
    # their downstream_id is the lineID they are on
    idx = barrier_joins.index.isin(
        barrier_segments.loc[barrier_segments.on_upstream].barrierID
    )
    barrier_joins.loc[idx, "downstream_id"] = barrier_joins.loc[idx].lineID

    ### Downstream endpoint barriers
    barrier_segments["on_downstream"] = (
        barrier_segments.linepos >= barrier_segments.length - EPS
    )

    print(
        "{:,} barriers on downstream point of their segments".format(
            len(barrier_segments.loc[barrier_segments.on_downstream])
        )
    )
    # their upstream_id is the lineID they are on
    idx = barrier_joins.index.isin(
        barrier_segments.loc[barrier_segments.on_downstream].barrierID
    )

    ### Split segments
    split_segments = barrier_segments.loc[
        ~(barrier_segments.on_upstream | barrier_segments.on_downstream)
    ]

    # ordinate the barriers by their projected distance on the line
    # Order this so we are always moving from upstream end to downstream end
    split_segments = split_segments.rename_axis("idx").sort_values(
        by=["idx", "linepos"], ascending=True
    )

    # join in count of barriers that SPLIT this segment
    split_segments = split_segments.join(
        split_segments.groupby(level=0).size().rename("barriers")
    )

    ##### Splits with 1 barrier per segment (easier!)
    # TODO: figure out reset_index or not??
    single_splits = split_segments.loc[split_segments.barriers == 1]  # .reset_index()

    print("{:,} segments split by a single barrier".format(len(single_splits)))

    geoms = (
        single_splits.apply(
            lambda row: cut_line_at_point(row.geometry, row.geometry_barrier), axis=1
        )
        .apply(pd.Series)
        .rename(columns={0: "upstream", 1: "downstream"})
    )

    single_splits = (
        single_splits[["lineID", "barrierID", "NHDPlusID"]]
        .join(geoms)
        .rename(columns={"lineID": "origLineID"})
    )

    new_segments = gp.GeoDataFrame(
        single_splits.melt(
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

    # Append new flowlines
    new_flowlines = prep_new_flowlines(flowlines, new_segments)
    updated_flowlines = no_barrier_segments.append(
        new_flowlines, ignore_index=True, sort=False
    ).set_index("lineID", drop=False)

    # join in data from flowlines
    # new_flowlines = new_segments.join(
    #     flowlines[["sizeclass", "streamorder"]], on="origLineID"
    # )

    # calculate length and sinuosity
    # new_flowlines["length"] = new_flowlines.length
    # new_flowlines["sinuosity"] = new_flowlines.geometry.apply(calculate_sinuosity)

    # Append new flowlines
    # updated_flowlines = no_barrier_segments.append(
    #     new_flowlines[
    #         [
    #             "lineID",
    #             "NHDPlusID",
    #             "sizeclass",
    #             "streamorder",
    #             "length",
    #             "sinuosity",
    #             "geometry",
    #         ]
    #     ],
    #     ignore_index=True,
    #     sort=False,
    # ).set_index("lineID", drop=False)

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

    # TODO: can be replaced with .update()?  Need indices to be set properly first
    barrier_joins = update_joins(barrier_joins, new_downstreams, new_upstreams)

    #### Multiple barriers per segment (harder)
    multi_splits = split_segments.loc[split_segments.barriers > 1]  # .reset_index()
    print("{:,} segments split by multiple barriers".format(len(multi_splits)))

    # FIXME:
    multi_splits = multi_splits.loc[multi_splits.lineID == 207026855]

    # Group barriers by line so that we can split geometries in one pass
    grouped = (
        multi_splits[
            [
                "lineID",
                "NHDPlusID",
                "barrierID",
                "barriers",
                "geometry",
                "geometry_barrier",
            ]
        ]
        .groupby("lineID")
        .agg(
            {
                "lineID": "first",
                "NHDPlusID": "first",
                "geometry": "first",
                "barrierID": list,
                "barriers": "first",
                "geometry_barrier": list,
            }
        )
    )

    # cut line for all barriers
    geoms = grouped.apply(
        lambda row: cut_line_at_points(row.geometry, row.geometry_barrier), axis=1
    )

    # create one column per segment
    barrier_cols = grouped.barrierID.apply(pd.Series)

    # pivot into multiple rows instead of columns
    stacked = (
        barrier_cols.stack()
        .reset_index()
        .rename(columns={"level_1": "barrierIdx", 0: "barrierID"})
        .set_index("lineID")
        # join in all segments to every barrier, we'll extract out the ones we want below
        .join(geoms.rename("geometry"))
        .reset_index()
        .set_index(["lineID", "barrierID"])
    )

    # extract segments per barrier
    # FIXME: logic error: should only have one segment per barrier not 2!
    # spb = stacked.apply(
    #     lambda row: row.geometry[row.barrierIdx : row.barrierIdx + 2], axis=1
    # )
        spb = stacked.apply(
        lambda row: row.geometry[row.barrierIdx : row.barrierIdx + 1], axis=1
    )

    spb = stacked.drop(columns=["geometry"]).join(
        spb.rename("geometry")
        .apply(pd.Series)
        .rename(columns={0: "upstream", 1: "downstream"})
    )

    # now have a segment pair (upstream, downstream) for each barrier
    # transform into multiple rows to assign new lineIDs
    new_segments = (
        spb.reset_index()
        .rename(columns={"lineID": "origLineID"})
        .melt(id_vars=["origLineID", "barrierID", "barrierIdx"], value_name="geometry")
        .sort_values(by=["origLineID", "barrierIdx"])
        .rename(columns={"variable": "side"})
        .reset_index(drop=True)
        .join(grouped[["barriers", "NHDPlusID"]], on="origLineID")
    )

    # label first and last barriers
    new_segments["position"] = "middle"
    new_segments.loc[new_segments.barrierIdx == 0, "position"] = "first"
    new_segments.loc[
        new_segments.barrierIdx == new_segments.barriers - 1, "position"
    ] = "last"

    # drop all upstream segments except for first postion, these are duplicates
    new_segments["duplicate"] = False
    new_segments.loc[
        (new_segments.side == "upstream") & (new_segments.position != "first"),
        "duplicate",
    ] = True

    new_segments = new_segments.loc[~new_segments.duplicate].reset_index(drop=True)

    # generate a new ID for each segment
    new_segments["lineID"] = next_segment_id + new_segments.index

    # Add in new flowlines
    new_flowlines = prep_new_flowlines(flowlines, new_segments)
    updated_flowlines.append(new_flowlines, ignore_index=True, sort=False).set_index(
        "lineID", drop=False
    )

    # update joins
    # NOTE: these are from the perspective of the line segment that was split
    # and only for the furthest barrier upstream or downstream per segment
    new_downstreams = (
        new_segments.loc[
            (new_segments.side == "upstream") & (new_segments.position == "first")
        ]
        .set_index("origLineID")
        .lineID.rename("new_downstream_id")
    )
    new_upstreams = (
        new_segments.loc[
            (new_segments.side == "downstream") & (new_segments.position == "last")
        ]
        .set_index("origLineID")
        .lineID.rename("new_upstream_id")
    )

    updated_joins = update_joins(updated_joins, new_downstreams, new_upstreams)

    # Add in new joins for the splits
    new_joins = new_segments[
        ["NHDPlusID", "lineID", "barrierID", "side", "position", "barrierIdx"]
    ]

    # add in joins across the barriers in this segment
    ids = (
        new_joins.loc[
            (new_joins.side == "downstream") & (new_joins.position != "first")
        ]
        .barrierID.copy()
        .values
    )
    idx = (new_joins.side == "downstream") & (new_joins.position != "last")
    new_joins.loc[idx, "position"] = "middle"
    internal_upstreams = new_joins.loc[idx].copy()
    internal_upstreams.side = "upstream"
    internal_upstreams.position = "middle"
    # set the ID for the barrier on the downstream side
    internal_upstreams.barrierID = ids

    new_joins = new_joins.append(internal_upstreams, ignore_index=True, sort=False)[
        ["NHDPlusID", "lineID", "barrierID", "side"]
    ].sort_values(by=["lineID", "barrierID"])

    # pivot to create columns for upstream / downstream ids
    new_joins = new_joins.pivot(index="barrierID", columns="side")
    new_joins.columns = ["downstream", "upstream", "downstream_id", "upstream_id"]
    new_joins["type"] = "internal"

    updated_joins = updated_joins.append(new_joins, ignore_index=True, sort=False)

    #### Update Barrier joins
    barrier_joins.update(new_joins)

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
