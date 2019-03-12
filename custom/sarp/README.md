# NHD Data Processing for Southeast Aquatic Barrier Inventory

## Purpose

The [Southeast Aquatic Resources Partnership](https://southeastaquatics.net) has created and maintained the most complete and value-added inventory of aquatic barriers in the Southeastern U.S. In order to evaluate the impact of barriers on aquatic organisms and to better prioritize barriers that would contribute the most high-quality connected habitat to the river and stream network if removed, it is necessary to calculate network connectivity metrics. The custom scripts here assisted with that processing.

Three types of barriers are considered in these analyses:

-   waterfalls: these are considered "hard" barriers that break the aquatic network
-   dams: these large barriers are considered "hard" barriers that break the aquatic network
-   small barriers (road / stream crossings): these barriers may or may not break the network, depending on site specific factors

These barriers are analyzed to produce 2 groups of outputs:

-   network metrics for dams, based on cutting the network for all dams and waterfalls
-   network metrics for small barriers, based on cutting the network for all dams, waterfalls, and small barriers

## Overall workflow:

1. Download NHDPlus HR data for all applicable HUC4s that have dams. Note: Region 8 is not available yet.
2. Get the barriers inventory from Kat at SARP
3. Run `prepare_nhd.py` for each applicable region
4. Run `merge.py` to merge HUC4s to regions and region groups for later processing
5. Run `create_spatial_indices.py` to create the spatial indices needed for later processing
6. Run `snap_dams_for_qa.py` to generate dams for manual QA. Send these to Kat. She moves points to get them in the right place, removes ones that are not present at all, and codes them as to known off-network dams. Right now, this is limited only to those likely to snap to the largest size classes in the network.
7. Run `prep_dams.py` to snap all QA'd dams to the network
8. Run `prep_small_barriers.py` to snap all small barriers to the network
9. Run `prep_waterfalls` to snap all waterfalls to the network.
10. Run `network_analysis.py` for the actual network analysis (cutting of segments, dissolving functional networks, network stats, etc)

## Detailed Workflow

See `README.md` in the project root for installation instructions.

Most processing is performed at the region level for large regions (e.g., Region 3) or for groups of adjacent regions that are smaller.

See `constants.py` for constants that are used to define the HUC4s extracted within a region and groupings of regions.

### Prepare of NHD data:

1. NHD High Resolution Plus (NHD HR) Data are downloaded from the NHD data distribution website by HUC4.
2. NHD data are extracted to an internal format using `prepare_nhd.py`. Enter the appropriate region number and output directory. This produces `flowlines.feather` and `flowline_joins.feather` for that region.
3. Adjacent regions are merged using `merge.py`. This produces `flowlines.feather` and `flowline_joins.feather` for that group.
4. Spatial indices are added to the merged flowlines in order to speed up operations such as snapping barriers to the flowlines. This is done with `create_spatial_indices.py`.

NHD data are now ready for further analysis.

The above steps should only need to be rerun if there are errors or additional HUC4s / regions are needed.

It can take roughly 0.5 to 3 hours to perform the above processing steps per region, depending on the complexity / size of the region.

### Prepare Barriers

In order to perform network analysis, barriers need to be snapped to the network. However, due to various data issues, incoming data may not snap correctly to the appropriate network segments, which could result in major impacts to the quality of the network analysis.

Three common issues may present:

-   very large dams may be represented using a coordinate that is quite a distance (>100m) from the nearest flowline, even though the full length of the dam intersects the flowlines. These generally need to be corrected by hand. Some value-added datasets, such as the National Anthropogenic Barrier database have performed some of this effort.
-   barriers may have been incorrectly digitized, and may fall too far from the appropriate flowline.
-   barriers near the junction of a tributary and a larger stream / river may snap to the wrong segment

In order to assist with barrier snapping and QA, the scripts `snap_dams_for_qa.py` and `snap_waterfalls_for_qa.py` will automatically attempt to snap barriers to the nearest segments and include heuristics about the quality of their likely match. These heuristics include the distance to the nearest segment, the number of segments within 100 meters, and a fuzzy match on the river / stream name to the GNIS name of the river / stream included in the barrier dataset.

At this point, manual snapping and QA is required in order to ensure that barriers are correctly assigned to the correct segments on the network.

1. Manually snapped dams are filtered and prepared using `prepare_dams.py`
2. Manually snapped small barriers (road / stream crossings) are prepared using `prepare_small_barriers.py`
3. Waterfalls are prepared using `prepare_waterfalls.py`

The above steps only need to be performed when there are updated data for each type of barrier.

### Prepare floodplain metrics

The amount of natural landcover in the floodplain of each aquatic network helps to measure the overall habitat quality of that network, and helps prioritize those barriers that if removed would contribute high quality upstream networks. In order to streamline processing for barrier inventories that growy over time, we approximated the natural landcover at the catchment level, so that floodplain statistics could be reused for many analyses rather than regenerated each time a new barrier is added to the inventory.

The floodplain statistics were generated in ArcGIS by:

1. developing a floodplain mask from existing data sources and 90m around all flowlines
2. developing a binary map of natural landcover / not natural landcover
3. clipping landcover by floodplain mask
4. running zonal stats to calculate the area in natural landcover and not natural landcover in the floodplain mask, for each catchment

Note: some catchments have no floodplain, and some have floodplains but no NHDPlusID (outside HUC4s we processed). These are filtered out.

These data were exported to a FGDB, and prepared for analysis here using `prepare_floodplain_stats.py`.

### Perform network analysis

Once all the inputs are prepared using the above steps, you can now perform the network analysis for that region or group of regions.

1. Run `network_analysis.py` once for dams (set `SMALL_BARRIERS = False`)
2. Run `network_analysis.py` again for small barriers (set `SMALL_BARRIERS = True`)

This can take 10 - 60+ minutes depending on the size and complexity of the region.

This cuts the network at each barrier and associates each barrier with an upstream and downstream flowline segment ID. It automatically calculates a new unique ID for a segment if it is creating a new segment between two barriers on the same original flowline segment. The networks are then re-assembled by traversing upstream from the downstream-most points of the NHD flowlines or from each barrier.

The output of this is a shapefile with one feature per functional network, a network summary statistics CSV, and a CSV including the upstream and downstream network IDs associated with each barrier.
