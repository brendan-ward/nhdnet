import pandas as pd
import geopandas as gp
import numpy as np
from time import time

from nhdnet.geometry.lines import (
    cut_line_at_points,
    cut_line_at_point,
    calculate_sinuosity,
)

# Points within 1 meter of the end are close enough not to cut,
# and instead get assigned to the endpoints
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
    """Add necessary attributes to new segments then append to flowlines and return.
    
    Calculates length and sinuosity for new segments.

    Parameters
    ----------
    flowlines : GeoDataFrame
        flowlines to append to
    new_segments : GeoDataFrame
        new segments to append to flowlines.
    
    Returns
    -------
    GeoDataFrame
    """
    # join in data from flowlines into new segments
    new_flowlines = new_segments.join(
        flowlines[["NHDPlusID", "sizeclass", "streamorder"]], on="origLineID"
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
    """Cut flowlines by barriers.
    
    Parameters
    ----------
    flowlines : GeoDataFrame
        ALL flowlines for region.
    barriers : GeoDataFrame
        Barriers that will be used to cut flowlines.
    joins : DataFrame
        Joins between flowlines (upstream, downstream pairs).
    next_segment_id : int, optional
        Used as starting point for IDs of new segments created by cutting flowlines.
    
    Returns
    -------
    GeoDataFrame, DataFrame, DataFrame
        updated flowlines, updated joins, barrier joins (upstream / downstream flowline ID per barrier)
    """

    print("Starting number of segments: {:,}".format(len(flowlines)))
    print("Cutting in {:,} barriers".format(len(barriers)))

    # Our segment ids are ints, so just increment from the last one we had from NHD
    if next_segment_id is None:
        next_segment_id = int(flowlines.index.max() + 1)

    # join barriers to lines and extract those that have segments (via inner join)
    barrier_segments = flowlines[["lineID", "NHDPlusID", "geometry"]].join(
        barriers[["geometry", "barrierID", "lineID"]].set_index("lineID", drop=False),
        rsuffix="_barrier",
        how="inner",
    )

    # Calculate the position of each barrier on each segment.
    # Barriers are on upstream or downstream end of segment if they are within
    # EPS of the ends.  Otherwise, they are splits
    barrier_segments["linepos"] = barrier_segments.apply(
        lambda row: row.geometry.project(row.geometry_barrier), axis=1
    )

    ### Upstream and downstream endpoint barriers
    barrier_segments["on_upstream"] = barrier_segments.linepos <= EPS
    barrier_segments["on_downstream"] = (
        barrier_segments.linepos >= barrier_segments.length - EPS
    )
    print(
        "{:,} barriers on upstream point of their segments\n{:,} barriers on downstream point of their segments".format(
            len(barrier_segments.loc[barrier_segments.on_upstream]),
            len(barrier_segments.loc[barrier_segments.on_downstream]),
        )
    )

    # Barriers on upstream endpoint:
    # their upstream_id is the upstream_id(s) of their segment from joins,
    # and their downstream_is is the segment they are on.
    # NOTE: a barrier may have multiple upstreams if it occurs at a fork in the network.
    # All terminal upstreams should be already coded as 0 in joins, but just in case
    # we assign N/A to 0.

    upstream_barrier_joins = (
        barrier_segments.loc[barrier_segments.on_upstream][["barrierID", "lineID"]]
        .rename(columns={"lineID": "downstream_id"})
        .join(joins.set_index("downstream_id").upstream_id, on="downstream_id")
    ).fillna(0)

    # Barriers on downstream endpoint:
    # their upstream_id is the segment they are on and their downstream_id is the
    # downstream_id of their segment from the joins.
    # Some downstream_ids may be missing if the barrier is on the downstream-most point of the
    # network (downstream terminal) and further downstream segments were removed due to removing
    # coastline segments.
    downstream_barrier_joins = (
        barrier_segments.loc[barrier_segments.on_downstream][["barrierID", "lineID"]]
        .rename(columns={"lineID": "upstream_id"})
        .join(joins.set_index("upstream_id").downstream_id, on="upstream_id")
    ).fillna(0)

    barrier_joins = upstream_barrier_joins.append(
        downstream_barrier_joins, ignore_index=True, sort=False
    ).set_index("barrierID", drop=False)

    ### Split segments have barriers that are not at endpoints

    split_segments = barrier_segments.loc[
        ~(barrier_segments.on_upstream | barrier_segments.on_downstream)
    ]
    # join in count of barriers that SPLIT this segment
    split_segments = split_segments.join(
        split_segments.groupby(level=0).size().rename("barriers")
    )

    print(
        "{:,} segments have one barrier\n{:,} segments have more than one barrier".format(
            len(split_segments.loc[split_segments.barriers == 1]),
            len(split_segments.loc[split_segments.barriers > 1]),
        )
    )

    # ordinate the barriers by their projected distance on the line
    # Order this so we are always moving from upstream end to downstream end
    split_segments = split_segments.rename_axis("idx").sort_values(
        by=["idx", "linepos"], ascending=True
    )

    # Group barriers by line so that we can split geometries in one pass
    grouped = (
        # multi_splits
        split_segments[
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

    # pivot list of geometries into rows and assign new IDs
    new_segments = gp.GeoDataFrame(
        geoms.apply(pd.Series)
        .stack()
        .reset_index()
        .rename(columns={0: "geometry", "lineID": "origLineID", "level_1": "i"})
    )

    new_segments["lineID"] = next_segment_id + new_segments.index

    # extract flowlines that are not split by barriers
    unsplit_segments = flowlines.loc[~flowlines.index.isin(split_segments.index)]

    # Add in new flowlines
    new_flowlines = prep_new_flowlines(flowlines, new_segments)
    updated_flowlines = unsplit_segments.append(
        new_flowlines, ignore_index=True, sort=False
    ).set_index("lineID", drop=False)

    # transform new segments to create new joins
    l = new_segments.groupby("origLineID").lineID
    # the first new line per original line is the furthest upstream, so use its
    # ID as the new downstream ID for anything that had this origLineID as its downstream
    first = l.first().rename("new_downstream_id")
    # the last new line per original line is the furthest downstream...
    last = l.last().rename("new_upstream_id")

    # Update existing joins with the new lineIDs we created at the upstream or downstream
    # ends of segments we just created
    updated_joins = update_joins(joins, first, last)

    # also need to update any barrier joins already created for those on endpoints
    barrier_joins = update_joins(barrier_joins, first, last)

    # create upstream & downstream ids per original line
    upstream_side = (
        new_segments.loc[~new_segments.lineID.isin(last)][["origLineID", "i", "lineID"]]
        .set_index(["origLineID", "i"])
        .rename(columns={"lineID": "upstream_id"})
    )

    downstream_side = new_segments.loc[~new_segments.lineID.isin(first)][
        ["origLineID", "i", "lineID"]
    ].rename(columns={"lineID": "downstream_id"})
    downstream_side.i = downstream_side.i - 1
    downstream_side = downstream_side.set_index(["origLineID", "i"])

    new_joins = (
        grouped.barrierID.apply(pd.Series)
        .stack()
        .reset_index()
        .rename(columns={"lineID": "origLineID", "level_1": "i", 0: "barrierID"})
        .set_index(["origLineID", "i"])
        .join(upstream_side)
        .join(downstream_side)
        .reset_index()
        .astype("uint32")
        .join(grouped.NHDPlusID.rename("upstream"), on="origLineID")
    )
    new_joins["downstream"] = new_joins.upstream
    new_joins["type"] = "internal"

    updated_joins = updated_joins.append(
        new_joins[["upstream", "downstream", "upstream_id", "downstream_id", "type"]],
        ignore_index=True,
        sort=False,
    ).sort_values(["downstream_id", "upstream_id"])

    barrier_joins = barrier_joins.append(
        new_joins[["barrierID", "upstream_id", "downstream_id"]],
        ignore_index=True,
        sort=False,
    ).set_index("barrierID", drop=False)

    return updated_flowlines, updated_joins, barrier_joins.astype("uint32")

