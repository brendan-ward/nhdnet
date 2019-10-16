# U.S. National Hydrography Dataset Network & Barrier Analysis Tools

[![Build Status](https://travis-ci.org/brendan-ward/nhdnet.svg?branch=master)](https://travis-ci.org/brendan-ward/nhdnet)
[![Coverage Status](https://coveralls.io/repos/github/brendan-ward/nhdnet/badge.svg?branch=master)](https://coveralls.io/github/brendan-ward/nhdnet?branch=master)

This library was used to help perform network connectivity analysis for the [Southeast Aquatic Barrier Prioritization Tool](https://connectivity.sarpdata.com). See [https://github.com/astutespruce/sarp-connectivity](https://github.com/astutespruce/sarp-connectivity) for more information about specific processing for that project.

This library is intended to provide more general processing utilities to assist with analyzing connectivity using the National Hydrography Dataset (NHD) - High Resolution Plus version.

We do not currently intend to add support for the NHD - Medium Resolution dataset. Pull requests are welcome to add this functionality.

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

-   reading / writing shapefiles using Geopandas can be very slow. We preferred to use the `feather` format for intermediate files (`geofeather` provides spatial compatibility).
-   data from NHD are downloaded as ArcGIS File Geodatabases. While these formats can be read (usually) using Geopandas, it is not possible to write this format, so shapefile outputs are generally the only option for use in GIS.

## Installation

`pip install nhdnet`

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

## Source Data

NHD High Resolution data are downloaded by HUC4 from [NHD Data Distribution Site](https://prd-tnm.s3.amazonaws.com/index.html?prefix=StagedProducts/Hydrography/NHDPlus/HU4/HighResolution/GDB/).

## Development

This project uses `black` for autoformatting and `pylint` for linting.

## Notes

Known issues:

-   flowlines that cross HUC4 or region boundaries need to be specifically handled as part of network analyses. See [https://github.com/astutespruce/sarp-connectivity/tree/master/analysis/network](https://github.com/astutespruce/sarp-connectivity/tree/master/analysis/network) for more information about network analysis based on this library.

## Credits

This project was made possible in partnership with the [Southeast Aquatic Resources Partnership](https://southeastaquatics.net) as part of a larger project to develop a comprehensive inventory of aquatic barriers in the Southeastern US and assess impacts of these barriers on aquatic systems.

The results of this project are available in the [Southeast Aquatic Barrier Prioritization Tool](https://connectivity.sarpdata.com).
