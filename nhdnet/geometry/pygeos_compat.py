"""Provide compatibility and basic spatial operations.

This is a shim ONLY until https://github.com/geopandas/geopandas/pull/1154
lands in GeoPandas.

The following operations are derived from the above PR.
These convert data through WKB, but with NO validation (see PR for validation)
"""

from pygeos import from_wkb, to_wkb
from shapely.wkb import loads


def to_pygeos(geoseries):
    return from_wkb(geoseries.apply(lambda g: g.wkb))


def from_pygeos(geometries):
    return geometries.apply(lambda g: loads(to_wkb(g)))

