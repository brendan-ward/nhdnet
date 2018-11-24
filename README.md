# US National Hydrography Dataset Barrier Analysis Tools

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

## WORK IN PROGRESS

This project is very much in progress and many kinks are still being worked out! Use with caution!

Known issues:

-   joins of flowlines that cross between HUC4s need to be handled

## Operation

Right now, the code is very specific to how we are analyzing data for the SE Barriers Inventory Project. We currently only
support using NHD High Resolution data.

NHD High Resolution data are downloaded by HUC4 from [NHD Data Distribution Site](https://prd-tnm.s3.amazonaws.com/index.html?prefix=StagedProducts/Hydrography/NHDPlus/HU4/HighResolution/GDB/).

Start by pre-processing NHD data:
Edit `/custom/sarp/prepare_nhd.py` to add the specific HUC4s you want, then run.

Then run the network analyis:
Edit `/custom/sarp/network_analysis.py` to add a HUC identified above, then run.
Run times vary from 2-20 minutes.

## Development

This project uses `black` for autoformatting and `pylint` for linting.

## Credits

This project was made possible in partnership with the [Southeast Aquatic Resources Partnership](https://southeastaquatics.net) as part of a larger project to develop a comprehensive inventory of aqautic barriers in the Southeastern US and assess impacts of these barriers on aquatic systems.
