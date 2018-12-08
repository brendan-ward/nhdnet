import pandas as pd
import numpy as np


def generate_network(root_id, upstreams):
    """Generate the upstream network from a starting id.

    Intended to be used within an .apply() call
    
    Parameters
    ----------
    root_id : id type (int, str)
        starting segment id that forms the root node of the upstream network
    upstreams : dict
        dictionary containing the list of upstream segment ids for each segment
    
    Returns
    -------
    list of all upstream ids in network traversing upward from root_id
    """

    ids = [root_id]
    network = []
    while ids:
        network.extend(ids)
        upstream_ids = []
        for id in ids:
            upstream_ids.extend(upstreams.get(id, []))
        ids = upstream_ids
    return network


def generate_networks(df, upstreams, column="upstream_id"):
    """Generate the upstream networks for each record in the input data frame.
    
    Parameters
    ----------
    df : pandas.DataFrame
        data frame containing the ids that are the root of each upstream network
    upstreams : dict
        dictionary containing the list of upstream segment ids for each segment
    column : str
        the name of the column containing the ids from which to traverse the network upstream
    
    Returns
    -------
    pandas.DataFrame
        contains all columns from df plus networkID.  Indexed on the values of df[column]
    """

    df["network"] = df[column].apply(lambda id: generate_network(id, upstreams))

    # Pivot the lists back into a flat data frame:
    # adapted from: https://stackoverflow.com/a/48532692
    return (
        pd.DataFrame(
            {
                c: np.repeat(df[c].values, df["network"].apply(len))
                for c in df.columns.drop("network")
            }
        )
        .assign(**{"network": np.concatenate(df["network"].values)})
        .rename(columns={column: "networkID"})
        .set_index("network")
    )


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
