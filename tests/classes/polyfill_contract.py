from unittest import TestCase

import geopandas as gpd
from shapely.geometry import LineString, Point, Polygon

from vector2dggs.indexerfactory import indexer_instance

from .base import skip_unless_backend

RES = {"h3": 8, "s2": 13, "a5": 17, "rhp": 8, "geohash": 6}


class TestPolyfillTokenContract(TestCase):
    """polyfill() must index by string cell ids for every backend."""

    def _assert_str_index(self, dggs):
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
        result = indexer_instance(dggs).polyfill(df, RES[dggs])
        self.assertGreater(len(result), 0)
        non_str = {type(c).__name__ for c in result.index if not isinstance(c, str)}
        self.assertFalse(non_str, f"non-str cell ids in index: {non_str}")

    def test_h3(self):
        self._assert_str_index("h3")

    def test_s2(self):
        self._assert_str_index("s2")

    def test_a5(self):
        self._assert_str_index("a5")

    def test_rhp(self):
        self._assert_str_index("rhp")

    def test_geohash(self):
        self._assert_str_index("geohash")
