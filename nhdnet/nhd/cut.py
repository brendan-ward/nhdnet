import pandas as pd
import geopandas as gp
import numpy as np

from nhdnet.geometry.lines import cut_line_at_points, calculate_sinuosity


# if points are within this distance of start or end coordinate, nothing is cut
EPS = 1e-6


def cut_flowlines(flowlines, barriers, joins):
    print("Starting number of segments: {}".format(len(flowlines)))
    print("Cutting in {0} barriers".format(len(barriers)))

    # Our segment ids are ints, so just increment from the last one we had from NHD
    next_segment_id = int(flowlines.index.max() + 1)

    update_cols = ["length", "sinuosity", "geometry", "lineID"]
    copy_cols = list(set(flowlines.columns).difference(update_cols))
    columns = copy_cols + update_cols

    # create container for new geoms
    new_flowlines = gp.GeoDataFrame(
        columns=flowlines.columns, crs=flowlines.crs, geometry="geometry"
    )

    updated_joins = joins.copy()

    # create join table for barriers
    barrier_joins = []

    segments_with_barriers = flowlines.loc[flowlines.index.isin(barriers.lineID)]
    print("{} segments have at least one barrier".format(len(segments_with_barriers)))
    for idx, row in segments_with_barriers.iterrows():
        # print("-----------------------\n\nlineID", idx)

        # Find upstream and downstream segments
        upstream_ids = joins.loc[joins.downstream_id == idx]
        downstream_ids = joins.loc[joins.upstream_id == idx]

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
                values = [row[c] for c in copy_cols] + [
                    segment.length,
                    calculate_sinuosity(segment),
                    segment,
                    id,
                ]
                new_flowlines.loc[id] = gp.GeoSeries(values, index=columns)

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

            # update upstream nodes to set first segment as their new downstream
            # update downstream nodes to set last segment as their new upstream
            updated_joins.loc[upstream_ids.index, "downstream_id"] = ids[0]
            updated_joins.loc[downstream_ids.index, "upstream_id"] = ids[-1]

            # add joins for everything after first node
            new_joins = [
                {
                    "upstream_id": ids[i],
                    "downstream_id": id,
                    "upstream": np.nan,
                    "downstream": np.nan,
                }
                for i, id in enumerate(ids[1:])
            ]
            updated_joins = updated_joins.append(new_joins, ignore_index=True)

        # If barriers are at the downstream-most point or upstream-most point
        us_points = points.loc[points.linepos <= EPS]
        ds_points = points.loc[points.linepos >= (length - EPS)]

        # Handle any points on the upstream or downstream end of line
        if len(us_points):
            # Create a record for each that has downstream set to the first segment if any, or
            # NHDPlusID of this segment
            # Do this for every upstream segment (if there are multiple upstream nodes)
            downstream_id = segments[0][0] if segments else idx
            for _, barrier in us_points.iterrows():
                for uIdx, upstream in upstream_ids.iterrows():
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
            for _, barrier in ds_points.iterrows():
                for _, downstream in downstream_ids.iterrows():
                    barrier_joins.append(
                        {
                            "NHDPlusID": row.NHDPlusID,
                            "joinID": barrier.joinID,
                            "upstream_id": upstream_id,
                            "downstream_id": downstream.downstream_id,
                        }
                    )

    # Drop all segments replaced by new segments
    flowlines = flowlines.drop(
        flowlines.loc[flowlines.NHDPlusID.isin(new_flowlines.NHDPlusID)].index
    )
    flowlines = flowlines.append(new_flowlines, sort=False)
    print("Final number of segments", len(flowlines))

    barrier_joins = pd.DataFrame(
        barrier_joins, columns=["NHDPlusID", "joinID", "upstream_id", "downstream_id"]
    )

    return flowlines, updated_joins, barrier_joins
