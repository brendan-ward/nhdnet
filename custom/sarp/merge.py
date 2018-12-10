import os
from time import time
from nhdnet.io import (
    serialize_gdf,
    deserialize_gdf,
    deserialize_df,
    serialize_df,
    to_shp,
)

from constants import REGIONS, REGION_GROUPS

src_dir = "/Users/bcward/projects/data/sarp/nhd"

start = time()

for group in REGION_GROUPS:
    group_start = time()
    print("-------- Group {} --------".format(group))

    out_dir = "{0}/region/{1}".format(src_dir, group)
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    merged = None
    merged_joins = None

    for HUC2 in REGION_GROUPS[group]:
        print("----------- Region {} ------------".format(HUC2))

        for i in REGIONS[HUC2]:
            HUC4 = "{0}{1:02d}".format(HUC2, i)
            huc_id = int(HUC4) * 1000000
            huc_dir = "{0}/{1}".format(src_dir, HUC4)

            print("Processing {}".format(HUC4))

            # Merge flowlines
            flowlines = deserialize_gdf("{}/flowline.feather".format(huc_dir))
            print("Read {} flowlines".format(len(flowlines)))
            flowlines["HUC4"] = HUC4
            flowlines["lineID"] += huc_id

            # TODO: this can be removed when flowlines are re-extracted
            flowlines.FType = flowlines.FType.astype("uint16")
            flowlines.StreamOrde = flowlines.StreamOrde.astype("uint8")

            if merged is None:
                merged = flowlines
            else:
                merged = merged.append(flowlines, ignore_index=True)

            # Merge joins
            joins = deserialize_df("{}/flowline_joins.feather".format(huc_dir))
            joins["HUC4"] = HUC4
            # TODO: this can be replaced when flowlines are re-extracted
            joins.upstream_id = joins.upstream_id.astype("uint32")
            joins.downstream_id = joins.downstream_id.astype("uint32")

            # Set updated lineIDs with the HUC4 prefix
            joins.loc[joins.upstream_id != 0, "upstream_id"] += huc_id
            joins.loc[joins.downstream_id != 0, "downstream_id"] += huc_id

            if merged_joins is None:
                merged_joins = joins
            else:
                merged_joins = merged_joins.append(joins, ignore_index=True)

    # TODO: redo this as a join
    # Update the missing upstream_ids at the joins between HUCs
    huc_in = merged_joins.loc[merged_joins.type == "huc_in"]
    for idx, row in huc_in.iterrows():
        match = merged_joins.loc[merged_joins.downstream == row.upstream].downstream_id
        if len(match):
            merged_joins.loc[idx, "upstream_id"] = match.iloc[0]

    # remove duplicate terminals
    merged_joins = merged_joins.loc[
        ~(
            merged_joins.upstream.isin(huc_in.upstream)
            & (merged_joins.type == "terminal")
        )
    ].copy()

    print("serializing {} flowlines to feather".format(len(merged)))
    serialize_gdf(merged, "{}/flowline.feather".format(out_dir))
    serialize_df(merged_joins, "{}/flowline_joins.feather".format(out_dir), index=False)

    print("serializing to shp")
    serialize_start = time()
    to_shp(merged, "{}/flowline.shp".format(out_dir))
    print("serialize done in {:.2f}".format(time() - serialize_start))

    print("Group Done in {:.2f}".format(time() - group_start))

print("All Done in {:.2f}".format(time() - start))
