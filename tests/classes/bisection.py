from unittest import TestCase

import geopandas as gpd
from shapely.geometry import Polygon

from vector2dggs.common import _run_bisection


class TestBisection(TestCase):
    """
    _run_bisection operates on a dataframe indexed by id_field, which is not
    guaranteed to be unique. Results must be written back to the row they were
    computed for, not to every row sharing an index label.
    """

    def test_duplicate_index_labels_do_not_cross_contaminate(self):
        big = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])
        small = Polygon([(20, 20), (20.1, 20), (20.1, 20.1), (20, 20.1)])
        df = gpd.GeoDataFrame({"geometry": [big, small]}, index=["a", "a"], crs=2193)
        df.index.name = "id"

        result = _run_bisection(df.copy(), 5.0, 1)

        # Only `big` exceeds the threshold and is bisected; its pieces must
        # preserve its area
        self.assertAlmostEqual(result.geometry.iloc[0].area, big.area, places=6)
        # `small` is under the threshold and must pass through untouched -
        # not be overwritten with big's bisection result
        self.assertAlmostEqual(result.geometry.iloc[1].area, small.area, places=9)

    def test_all_oversized_duplicate_labels_each_keep_own_geometry(self):
        square_a = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])
        square_b = Polygon([(100, 100), (110, 100), (110, 110), (100, 110)])
        df = gpd.GeoDataFrame(
            {"geometry": [square_a, square_b]}, index=["a", "a"], crs=2193
        )
        df.index.name = "id"

        result = _run_bisection(df.copy(), 5.0, 1)

        self.assertAlmostEqual(result.geometry.iloc[0].area, square_a.area, places=6)
        self.assertAlmostEqual(result.geometry.iloc[1].area, square_b.area, places=6)
        # The two rows were bisected independently and must not be identical
        self.assertFalse(result.geometry.iloc[0].equals(result.geometry.iloc[1]))
