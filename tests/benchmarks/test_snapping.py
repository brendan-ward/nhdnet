import pytest

from nhdnet.geometry.lines import snap_to_line, snap_to_line_old


@pytest.mark.benchmark(group="snapping")
def test_snap_to_line(flowlines, road_crossings, benchmark):
    benchmark(snap_to_line, road_crossings, flowlines, tolerance=100)


@pytest.mark.benchmark(group="snapping")
def test_snap_to_line_old(flowlines, road_crossings, benchmark):
    benchmark(snap_to_line_old, road_crossings, flowlines, tolerance=100)
