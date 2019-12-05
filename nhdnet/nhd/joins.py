def find_join(df, id, downstream_col="downstream", upstream_col="upstream"):
    """Find the joins for a given segment id in a joins table.

    Parameters
    ----------
    df : DataFrame
        data frame containing the joins
    id : any
        id to lookup in upstream or downstream columns
    downstream_col : str, optional (default "downstream")
        name of downstream column
    upstream_col : str, optional (default "upstream")
        name of upstream column

    Returns
    -------
    Joins that have the id as an upstream or downstream.
    """
    return df.loc[(df[upstream_col] == id) | (df[downstream_col] == id)]


def find_joins(df, ids, downstream_col="downstream", upstream_col="upstream"):
    """Find the joins for a given segment id in a joins table.

    Parameters
    ----------
    df : DataFrame
        data frame containing the joins
    ids : list-like
        ids to lookup in upstream or downstream columns
    downstream_col : str, optional (default "downstream")
        name of downstream column
    upstream_col : str, optional (default "upstream")
        name of upstream column

    Returns
    -------
    Joins that have the id as an upstream or downstream.
    """
    return df.loc[(df[upstream_col].isin(ids)) | (df[downstream_col].isin(ids))]


def index_joins(df, downstream_col="downstream", upstream_col="upstream"):
    """Create an index of joins based on a given segment id.
    Returns a dataframe indexed by id, listing the id of the next
    segment upstream and the id of the next segment downstream.

    Segments that have 0 for both upstream and downstream are 1 segment long.

    WARNING: 0 has special meaning, it denotes that there is no corresponding
    segment.

    Parameters
    ----------
    df : DataFrame
        data frame containing the joins.
    downstream_col : str, optional (default "downstream")
        downstream column name
    upstream_col : str, optional (default "upstream")
        upstream column name

    Returns
    -------
    DataFrame
    """

    return (
        df.loc[df[downstream_col] != 0]
        .set_index(downstream_col)[[upstream_col]]
        .join(df.set_index(upstream_col)[[downstream_col]])
        .reset_index()
        .drop_duplicates()
        .set_index("index")
    )


def create_upstream_index(
    df, downstream_col="downstream", upstream_col="upstream", exclude=None
):
    """Create an index of downstream ids to all their respective upstream ids.
    This is so that network traversal can start from a downstream-most segment,
    and then traverse upward for all segments that have that as a downstream segment.

    Parameters
    ----------
    df : DataFrame
        Data frame containing the pairs of upstream_col and downstream_col that
        represent the joins between segments.
    downstream_col : str, optional (default "downstream")
        Name of column containing downstream ids
    upstream_col : str, optional (default "upstream")
        Name of column containing upstream ids
    exclude : list-like, optional (default None)
        List-like containing segment ids to exclude from the list of upstreams.
        For example, barriers that break the network should be in this list.
        Otherwise, network traversal will operate from the downstream-most point
        to all upstream-most points, which can be very large for some networks.

    Returns
    -------
    dict
        dictionary of downstream_id to the corresponding upstream_id(s)
    """

    ix = (df[upstream_col] != 0) & (df[downstream_col] != 0)

    if exclude is not None:
        ix = ix & (~df[upstream_col].isin(exclude))

    # NOTE: this looks backward but is correct for the way that grouping works.
    return (
        df[ix, [downstream_col, upstream_col]]
        .set_index(upstream_col)
        .groupby(downstream_col)
        .groups
    )


def remove_joins(df, ids, downstream_col="downstream", upstream_col="upstream"):
    """Remove any joins to or from ids.
    This sets any joins that terminate downstream to one of these ids to 0 in order to mark them
    as new downstream terminals.  A join that includes other downstream ids not in ids will be left as is.

    Parameters
    ----------
    df : DataFrame
        Data frame containing the pairs of upstream_col and downstream_col that
        represent the joins between segments.
    ids : list-like
        List of ids to remove from the joins
    downstream_col : str, optional (default "downstream")
        Name of column containing downstream ids
    upstream_col : str, optional (default "upstream")
        Name of column containing upstream ids

    Returns
    -------
    [type]
        [description]
    """
    # TODO: fix new dangling terminals?  Set to 0 first?
    # join_df = join_df.loc[~join_df.upstream.isin(coastline_idx)].copy()

    # set the downstream to 0 for any that join coastlines
    # this will enable us to mark these as downstream terminals in
    # the network analysis later
    # join_df.loc[join_df.downstream.isin(coastline_idx), "downstream"] = 0

    # drop any duplicates (above operation sets some joins to upstream and downstream of 0)
    # join_df = join_df.drop_duplicates()

    return df.loc[~(df[upstream_col].isin(ids) | (df[downstream_col].isin(ids)))].copy()
