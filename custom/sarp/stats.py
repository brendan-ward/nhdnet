def calculate_network_stats(df):
    # for every network, calc length-weighted sinuosity and sum length, pct natural landcover
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

    # Sum floodplain and natural floodplain values, and calculate percent natural floodplain
    pct_nat_df = (
        df[["networkID", "floodplain_km2", "nat_floodplain_km2"]]
        .groupby("networkID")
        .sum()
        .reset_index()
        .set_index("networkID")
    )

    pct_nat_df["PctNatFloodplain"] = (
        100 * pct_nat_df.nat_floodplain_km2 / pct_nat_df.floodplain_km2
    ).astype("float32")

    segment_count_df = (
        df[["networkID"]]
        .groupby("networkID")
        .size()
        .reset_index()
        .set_index("networkID")
        .rename(columns={0: "count"})
    )

    stats_df = (
        sum_length_df.join(wtd_sinuosity_df)
        .join(num_sc_df)
        .join(segment_count_df)
        .join(pct_nat_df[["PctNatFloodplain"]])
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

    return stats_df[
        [
            "km",
            "miles",
            "NetworkSinuosity",
            "NumSizeClassGained",
            "PctNatFloodplain",
            "count",
        ]
    ]
