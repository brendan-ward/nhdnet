def find_join(df, id, upstream="upstream", downstream="downstream"):
    """Find the joins for a given segment id in a joins table.

    Parameters
    ----------
    df : DataFrame
        data frame containing the joins
    id : any
        id to lookup in upstream or downstream columns
    upstream : str, optional (default "upstream")
        name of upstream column
    downstream : str, optional (default "downstream")
        name of downstream column

    Returns
    -------
    Joins that have the id as an upstream or downstream.
    """
    return df.loc[(df[upstream] == id) | (df[downstream] == id)]


def find_joins(df, ids, upstream="upstream", downstream="downstream"):
    """Find the joins for a given segment id in a joins table.

    Parameters
    ----------
    df : DataFrame
        data frame containing the joins
    ids : list-like
        ids to lookup in upstream or downstream columns
    upstream : str, optional (default "upstream")
        name of upstream column
    downstream : str, optional (default "downstream")
        name of downstream column

    Returns
    -------
    Joins that have the id as an upstream or downstream.
    """
    return df.loc[(df[upstream].isin(ids)) | (df[downstream].isin(ids))]


# def find_singles(df, upstream="upstream", downstream="downstream"):
#     return df.loc[
#         ((df[upstream] == 0) & (df[downstream != 0]))
#         | ((df[downstream] == 0) & (df[upstream] != 0))
#     ]


def index_joins(df, upstream="upstream", downstream="downstream"):
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
    upstream : str, optional (default "upstream")
        upstream column name
    downstream : str, optional (default "downstream")
        downstream column name

    Returns
    -------
    DataFrame
    """

    return (
        df.loc[df[downstream] != 0]
        .set_index(downstream)[[upstream]]
        .join(df.set_index(upstream)[[downstream]])
        .reset_index()
        .drop_duplicates()
        .set_index("index")
    )

