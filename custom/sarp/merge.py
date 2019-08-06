from pathlib import Path
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

huc4_dir = Path("../data/sarp/derived/nhd/huc4")
out_dir = Path("../data/sarp/derived/nhd/region")

start = time()

for group in REGION_GROUPS:
    group_start = time()
    print("-------- Group {} --------".format(group))

    region_dir = out_dir / group
    if not os.path.exists(region_dir):
        os.makedirs(region_dir)

    if os.path.exists(region_dir / "flowline.feather"):
        print("Skipping existing region {}".format(group))
        continue

    merged = None
    merged_joins = None

    for HUC2 in REGION_GROUPS[group]:
        print("----------- Region {} ------------".format(HUC2))

        for i in REGIONS[HUC2]:
            HUC4 = "{0}{1:02d}".format(HUC2, i)
            huc_id = int(HUC4) * 1000000

            print("Processing {}".format(HUC4))

            # Merge flowlines
            flowlines = deserialize_gdf(huc4_dir / HUC4 / "flowline.feather")
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
            joins = deserialize_df(huc4_dir / HUC4 / "flowline_joins.feather")
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
    serialize_gdf(merged, region_dir / "flowline.feather")
    serialize_df(merged_joins, region_dir / "flowline_joins.feather", index=False)

    print("serializing to shp")
    serialize_start = time()
    to_shp(merged, region_dir / "flowline.shp")
    print("serialize done in {:.2f}".format(time() - serialize_start))

    print("Group Done in {:.2f}".format(time() - group_start))

print("All Done in {:.2f}".format(time() - start))
