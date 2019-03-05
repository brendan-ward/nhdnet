# U.S. National Hydrography Dataset Barrier Analysis Tools

This library was used to help perform network connectivity analysis for the [Southeast Aquatic Barrier Prioritization Tool](https://connectivity.sarpdata.com). See `custom/sarp/README.md` for more information about specific processing for that project.

This library is intended to provide more general processing utilities to assist with analyzing connectivity using the National Hydrography Dataset (NHD) - High Resolution Plus version.

We do not currently intend to add support for the NHD - Medum Resolution dataset. Pull requests are welcome to add this functionality.

Due to the large size of NHD data, it may be possible to only process a single region at a time, or a group of regions. The key limits are based on the amount of available memory (RAM) and the file sizes of the outputs (shapefiles are limited to 2 GB in size).

Key features:

-   preprocessing utilities to prepare NHD data for analysis within this library
-   merging of NHD flowlines between adjacent basins or regions
-   automatic snapping barriers to nearest flowlines, including heuristics to aid with manual QA/QC
-   cutting of NHD flowlines at barriers
-   construction of functional upstream networks from a barrier to the next upstream barriers or origins of a stream network
-   network statistics
-   optimized data I/O using the `feather` file format for intermediate data products and customized serialization / deserialization of spatial data

Notes:

-   reading / writing shapefiles using Geopandas can be very slow. We preferred to use the `feather` format and custom packaging of spatial data for internal processing steps to greatly speed up data processing.
-   data from NHD are downloaded as ArcGIS File Geodatabases. While these formats can be read (usually) using Geopandas, it is not possible to write this format, so shapefile outputs are generally the only option for use in GIS.

## Installation

This project uses [`GeoPandas`](http://geopandas.org/), [`Pandas`](https://pandas.pydata.org/), [`rtree`](http://toblerity.org/rtree/), and [`shapely`](https://shapely.readthedocs.io/en/stable/) in Python 3.6+.

We do not intend to support Python < 3.6.

Due to the complexity of these libraries, installation instructions for your platform may vary from the following.

`rtree` first requires the separate installation of [`libspatialindex`](http://libspatialindex.github.io/).
On MacOS:

```
brew install spatialindex
```

Python dependencies and virtual environment are managed using [`pipenv`](https://pipenv.readthedocs.io/en/latest/).

```
pipenv install
```

If you do not wish to use `pipenv`, see the `Pipfile` for the list of dependencies.

## Operation

NHD High Resolution data are downloaded by HUC4 from [NHD Data Distribution Site](https://prd-tnm.s3.amazonaws.com/index.html?prefix=StagedProducts/Hydrography/NHDPlus/HU4/HighResolution/GDB/).

Start by pre-processing NHD data:
Edit `/custom/sarp/prepare_nhd.py` to add the specific HUC4s you want, then run.

Then run the network analyis:
Edit `/custom/sarp/network_analysis.py` to add a HUC identified above, then run.
Run times vary from 2-20 minutes.

## Development

This project uses `black` for autoformatting and `pylint` for linting.

## WORK IN PROGRESS

This project is very much in progress and many kinks are still being worked out! Use with caution!

Known issues:

-   joins of flowlines that cross between HUC4s need to be handled

## Credits

This project was made possible in partnership with the [Southeast Aquatic Resources Partnership](https://southeastaquatics.net) as part of a larger project to develop a comprehensive inventory of aqautic barriers in the Southeastern US and assess impacts of these barriers on aquatic systems.

The results of this project are available in the [Southeast Aquatic Barrier Prioritization Tool](https://connectivity.sarpdata.com).
