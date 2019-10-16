import os
from pathlib import Path
from geofeather import from_geofeather
from pytest import fixture


fixtures_dir = Path(__file__).resolve().parent / "fixtures"


@fixture(scope="session")
def flowlines():
    return from_geofeather(fixtures_dir / "nhd_flowlines.feather")


@fixture(scope="session")
def road_crossings():
    return from_geofeather(fixtures_dir / "road_xings.feather")

