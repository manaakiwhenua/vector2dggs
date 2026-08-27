from unittest import TestCase

import geopandas as gpd
import pyarrow as pa
from shapely.geometry import LineString, Point, Polygon

from vector2dggs.indexerfactory import indexer_instance

from .base import skip_unless_backend

RES = {"h3": 8, "s2": 13, "a5": 17, "rhp": 8, "geohash": 6}
# fine enough that multiple cell centres fall inside the test hole
HOLE_RES = {"h3": 10, "s2": 15, "a5": 18, "rhp": 9, "geohash": 7}


class TestPolyfillTokenContract(TestCase):
    """
    polyfill() must index by each backend's own working cell-id form: str
    for backends with CELL_ARROW_TYPE == string (all of them, until #199/
    #200 land), int for a backend with a native form (#198: A5). This is
    always the working form, never mode-dependent - string is a one-time
    output-boundary rendering, not something polyfill() itself produces.
    """

    def _assert_native_index(self, dggs):
        skip_unless_backend(dggs)
        # matches the shape polyfill() receives in the pipeline: feature id
        # as a column, plain RangeIndex
        df = gpd.GeoDataFrame(
            {
                "fid": [1, 2, 3],
                "geometry": [
                    Polygon(
                        [
                            (174.70, -41.30),
                            (174.75, -41.30),
                            (174.75, -41.25),
                            (174.70, -41.25),
                        ]
                    ),
                    LineString([(174.70, -41.30), (174.75, -41.25)]),
                    Point(174.72, -41.28),
                ],
            },
            crs=4326,
        )
        indexer = indexer_instance(dggs)
        result = indexer.polyfill(df, RES[dggs])
        self.assertGreater(len(result), 0)
        expected_type = str if pa.string() == indexer.CELL_ARROW_TYPE else int
        wrong = {
            type(c).__name__ for c in result.index if not isinstance(c, expected_type)
        }
        self.assertFalse(wrong, f"unexpected cell id type in index: {wrong}")

    def _assert_hole_respected(self, dggs):
        skip_unless_backend(dggs)
        shell = [(174.70, -41.30), (174.76, -41.30), (174.76, -41.24), (174.70, -41.24)]
        hole = [(174.72, -41.28), (174.74, -41.28), (174.74, -41.26), (174.72, -41.26)]
        donut = Polygon(shell, [hole])
        # shrunk oracle: backends interpret the hole's straight edges as
        # curves (sub-metre band), so only centres well inside the hole count
        hole_core = Polygon(hole).buffer(-0.001)
        df = gpd.GeoDataFrame({"fid": [1], "geometry": [donut]}, crs=4326)
        indexer = indexer_instance(dggs)
        result = indexer.polyfill(df, HOLE_RES[dggs])
        self.assertGreater(len(result), 0)
        leaked = [
            c for c in result.index if hole_core.contains(indexer.cell_to_point(c))
        ]
        self.assertFalse(leaked, f"cells with centres inside the hole: {leaked[:5]}")

    def test_h3(self):
        self._assert_native_index("h3")
        self._assert_hole_respected("h3")

    def test_s2(self):
        self._assert_native_index("s2")
        self._assert_hole_respected("s2")

    def test_a5(self):
        self._assert_native_index("a5")
        self._assert_hole_respected("a5")

    def test_rhp(self):
        self._assert_native_index("rhp")
        self._assert_hole_respected("rhp")

    def test_geohash(self):
        self._assert_native_index("geohash")
        self._assert_hole_respected("geohash")
