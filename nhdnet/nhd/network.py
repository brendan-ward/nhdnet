# TODO: do we need igraph?  http://igraph.org/python/doc/tutorial/tutorial.html
# Also see ideas here: http://matthewrocklin.com/blog/work/2017/09/21/accelerating-geopandas-1
import pandas as pd


def generate_network(root_id, upstreams):
    ids = [root_id]
    network = []
    while ids:
        network.extend(ids)
        upstream_ids = []
        for id in ids:
            upstream_ids.extend(upstreams.get(id, []))
        ids = upstream_ids
    return network


def calculate_network_stats(df):
    # for every network, calc length-weighted sinuosity and sum length
    sum_length_df = (
        df[["networkID", "length"]]
        .groupby(["networkID"])
        .sum()
        .reset_index()
        .set_index("networkID")
    )
    temp_df = df.join(sum_length_df, on="networkID", rsuffix="_total")
    temp_df["wtd_sinuosity"] = temp_df.sinuosity * (
        temp_df.length / temp_df.length_total
    )

    wtd_sinuosity_df = (
        temp_df[["networkID", "wtd_sinuosity"]]
        .groupby(["networkID"])
        .sum()
        .reset_index()
        .set_index("networkID")
    )

    num_sc_df = (
        df[["networkID", "sizeclass"]]
        .groupby("networkID")
        .sizeclass.nunique()
        .reset_index()
        .set_index("networkID")
    )
    num_sc_df = num_sc_df - 1  # subtract the size class we are on

    stats_df = (
        sum_length_df.join(wtd_sinuosity_df)
        .join(num_sc_df)
        .rename(
            columns={
                "wtd_sinuosity": "NetworkSinuosity",
                "sizeclass": "NumSizeClassGained",
            }
        )
    )

    # convert units
    stats_df["km"] = stats_df.length / 1000.0
    stats_df["miles"] = stats_df.length * 0.000621371

    return stats_df[["km", "miles", "NetworkSinuosity", "NumSizeClassGained"]]


# DEPRECATED: older, slower implementation
# Depends on:

# get_upstream_ids = lambda id: upstreams.loc[id].upstream_id
# def get_upstream_ids(id):
#     ids = upstreams.loc[id]
#     if isinstance(ids, pd.Series):
#         return ids
#     # in case we got a single result back
#     return [ids]

# downstreams = join_ids.groupby("upstream_id")["downstream_id"].size()
# has_multiple_downstreams = lambda id: downstreams.loc[id] > 1


def generate_network_recursive(
    id, get_upstream_ids, has_multiple_downstreams, stop_segments={}
):
    # print("segment {}".format(id))
    ret = [id]

    upstream_ids = get_upstream_ids(id)

    for upstream_id in upstream_ids:
        if upstream_id == 0:  # Origin
            continue

        if upstream_id in stop_segments:
            continue

        if has_multiple_downstreams(upstream_id):
            # Make sure that we don't pass through this one multiple times!
            stop_segments.add(upstream_id)

        upstream_network = generate_network_recursive(
            upstream_id, get_upstream_ids, has_multiple_downstreams, stop_segments
        )
        ret.extend(upstream_network)

    return ret
