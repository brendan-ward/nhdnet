import pandas as pd
import geopandas as gp
import numpy as np

from nhdnet.geometry.lines import cut_line_at_points, calculate_sinuosity

# if points are within this distance of start or end coordinate, nothing is cut
EPS = 1e-6


# TODO: can call this in a loop of HUC4s to keep the size down?
def cut_flowlines(flowlines, barriers, joins, next_segment_id=None):
    print("Starting number of segments: {}".format(len(flowlines)))
    print("Cutting in {0} barriers".format(len(barriers)))

    # Our segment ids are ints, so just increment from the last one we had from NHD
    if next_segment_id is None:
        next_segment_id = int(flowlines.index.max() + 1)

    # origLineID is the original lineID of the segment that was split
    # can be used to join back to flowlines to get the other props
    columns = ["length", "sinuosity", "geometry", "lineID", "origLineID"]

    updated_upstreams = dict()
    updated_downstreams = dict()

    barrier_joins = []
    new_segments = []
    new_joins = []

    segments_with_barriers = flowlines.loc[flowlines.index.isin(barriers.lineID)][
        ["NHDPlusID", "geometry"]
    ]
    print("{} segments have at least one barrier".format(len(segments_with_barriers)))
    for row in segments_with_barriers.itertuples():
        idx = row.Index
        # print("-----------------------\n\nlineID", idx)

        # Find upstream and downstream segments
        upstreams = joins.loc[joins.downstream_id == idx]
        downstreams = joins.loc[joins.upstream_id == idx]

        # Barriers on this line
        points = barriers.loc[barriers.lineID == idx].copy()

        # ordinate the barriers by their projected distance on the line
        # Order this so we are always moving from upstream end to downstream end
        line = row.geometry
        length = line.length
        points["linepos"] = points.geometry.apply(lambda p: line.project(p))
        points.sort_values("linepos", inplace=True, ascending=True)

        # by definition, splits must occur after the first coordinate in the line and before the last coordinate
        split_points = points.loc[
            (points.linepos > EPS) & (points.linepos < (length - EPS))
        ]

        segments = []
        if len(split_points):
            # print(length, split_points.linepos)
            lines = cut_line_at_points(line, split_points.geometry)
            num_segments = len(lines)
            ids = list(range(next_segment_id, next_segment_id + num_segments))
            next_segment_id += num_segments

            # make id, segment pairs
            segments = list(zip(ids, lines))

            # add these to flowlines
            for i, (id, segment) in enumerate(segments):
                values = gp.GeoSeries(
                    [segment.length, calculate_sinuosity(segment), segment, id, idx],
                    index=columns,
                )
                new_segments.append(values)

                if i < num_segments - 1:
                    # add a join for this barrier
                    barrier_joins.append(
                        {
                            "NHDPlusID": row.NHDPlusID,
                            "joinID": split_points.iloc[i].joinID,
                            "upstream_id": id,
                            "downstream_id": ids[i + 1],
                        }
                    )

            # Since are UPDATING the downstream IDs of the upstream segments, we build a mapping of the original
            # downstream_id to the updated value for the new segment.
            # sets downstream_id
            updated_downstreams.update({id: ids[0] for id in upstreams.downstream_id})
            # sets upstream_id
            updated_upstreams.update({id: ids[-1] for id in downstreams.upstream_id})

            # # add joins for everything after first node
            new_joins.extend(
                [
                    {
                        "upstream_id": ids[i],
                        "downstream_id": id,
                        "upstream": np.nan,
                        "downstream": np.nan,
                    }
                    for i, id in enumerate(ids[1:])
                ]
            )

        # If barriers are at the downstream-most point or upstream-most point
        us_points = points.loc[points.linepos <= EPS]
        ds_points = points.loc[points.linepos >= (length - EPS)]

        # Handle any points on the upstream or downstream end of line
        if len(us_points):
            # Create a record for each that has downstream set to the first segment if any, or
            # NHDPlusID of this segment
            # Do this for every upstream segment (if there are multiple upstream nodes)
            downstream_id = segments[0][0] if segments else idx
            for barrier in us_points.itertuples():
                for upstream in upstreams.itertuples():
                    barrier_joins.append(
                        {
                            "NHDPlusID": row.NHDPlusID,
                            "joinID": barrier.joinID,
                            "upstream_id": upstream.upstream_id,
                            "downstream_id": downstream_id,
                        }
                    )

        if len(ds_points):
            # Create a record for each that has upstream set to the last segment if any, or
            # NHDPlusID of this segment
            # Do this for every downstream segment (if there are multiple downstream nodes)
            upstream_id = segments[-1][0] if segments else idx
            for barrier in ds_points.itertuples():
                for downstream in downstreams.itertuples():
                    barrier_joins.append(
                        {
                            "NHDPlusID": row.NHDPlusID,
                            "joinID": barrier.joinID,
                            "upstream_id": upstream_id,
                            "downstream_id": downstream.downstream_id,
                        }
                    )

    new_flowlines = gp.GeoDataFrame(
        new_segments, columns=columns, crs=flowlines.crs, geometry="geometry"
    )
    new_flowlines = new_flowlines.join(
        flowlines.drop(columns=["length", "sinuosity", "geometry", "lineID"]),
        on="origLineID",
    )

    # Drop all segments replaced by new segments
    # Join to new flowlines after reseting index to make append easier
    # Then add the index back
    flowlines = (
        flowlines.loc[~flowlines.index.isin(new_flowlines.origLineID.unique())]
        .reset_index(drop=True)
        .append(
            new_flowlines.drop(columns=["origLineID"]), sort=False, ignore_index=True
        )
    )
    # reset types
    flowlines.lineID = flowlines.lineID.astype("uint32")
    flowlines.set_index("lineID", drop=False, inplace=True)

    print("Final number of segments", len(flowlines))

    # Append in new joins
    joins = joins.append(pd.DataFrame(new_joins), ignore_index=True, sort=False)

    # update joins that had new segments inserted between them
    index = joins.upstream_id.isin(updated_upstreams.keys())
    joins.loc[index, "upstream_id"] = joins.loc[index].upstream_id.map(
        updated_upstreams
    )

    index = joins.downstream_id.isin(updated_downstreams.keys())
    joins.loc[index, "downstream_id"] = joins.loc[index].downstream_id.map(
        updated_downstreams
    )

    joins.upstream_id = joins.upstream_id.astype("uint32")
    joins.downstream_id = joins.downstream_id.astype("uint32")

    barrier_joins = pd.DataFrame(
        barrier_joins, columns=["NHDPlusID", "joinID", "upstream_id", "downstream_id"]
    )

    # Update barrier joins based on upstreams or downstreams that were updated with
    # new segment ids
    index = barrier_joins.upstream_id.isin(updated_upstreams.keys())
    barrier_joins.loc[index, "upstream_id"] = barrier_joins.loc[index].upstream_id.map(
        updated_upstreams
    )

    index = barrier_joins.downstream_id.isin(updated_downstreams.keys())
    barrier_joins.loc[index, "downstream_id"] = barrier_joins.loc[
        index
    ].downstream_id.map(updated_downstreams)

    return flowlines, joins, barrier_joins
