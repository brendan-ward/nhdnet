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
        Dictionary created from Pandas groupby().groups - keys are downstream_ids, values are upstream_ids
    
    Returns
    -------
    list of all upstream ids in network traversing upward from root_id
    """

    network = [root_id]
    ids = [root_id]
    while len(ids):
        upstream_ids = []
        for id in ids:
            upstream_ids.extend(upstreams.get(id, []))

        ids = upstream_ids
        network.extend(ids)

    return network


def generate_networks(root_ids, upstreams):
    """Generate the upstream networks for each root ID in root_ids.
    IMPORTANT: this will produce multiple upstream networks from a given starting point
    if the starting point is located at the junction of multiple upstream networks.
    
    Parameters
    ----------
    root_ids : pandas.Series
        Series of root IDs (downstream-most ID) for each network to be created
    upstreams : dict
        Dictionary created from Pandas groupby().groups - keys are downstream_ids, values are upstream_ids
    
    Returns
    -------
    pandas.DataFrame
        Contains networkID based on the value in root_id for each network, and the associated lineIDs in that network
    """

    # create the list of upstream segments per root ID
    network_segments = root_ids.apply(
        lambda id: generate_network(id, upstreams)
    ).rename("lineID")

    # transform into a flat dataframe, with one entry per lineID in each network
    return pd.DataFrame(
        {
            "networkID": np.repeat(root_ids, network_segments.apply(len)),
            "lineID": np.concatenate(network_segments.values),
        }
    )
