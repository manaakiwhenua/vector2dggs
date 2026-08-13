from unittest import TestCase

import geopandas as gpd
import pyproj
from shapely.geometry import Polygon

from vector2dggs import common
from vector2dggs import constants as const
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


class TestBisectionPreparation(TestCase):
    """
    Default cut_threshold derivation must be exact for any CRS unit (via
    pyproj unit conversion factors), and --cut_crs must take effect even
    when no explicit threshold is given.
    """

    SQUARE_4167 = Polygon([(174, -41), (174.1, -41), (174.1, -40.9), (174, -40.9)])

    def _prep(self, cut_crs=None, cut_threshold=None):
        df = gpd.GeoDataFrame({"geometry": [self.SQUARE_4167]}, crs=4167)
        return common.bisection_preparation(df, "h3", 5, cut_crs, cut_threshold)

    def test_cut_crs_applies_without_explicit_threshold(self):
        target = pyproj.CRS.from_epsg(2193)
        df, cut_crs, threshold = self._prep(cut_crs=target)
        self.assertEqual(df.crs, target)
        self.assertEqual(cut_crs, target)
        # metre CRS: threshold is the parent-res cell area in m^2
        self.assertAlmostEqual(
            threshold, const.DEFAULT_AREA_THRESHOLD_M2("h3", 5), delta=1
        )

    def test_default_threshold_in_foot_crs(self):
        target = pyproj.CRS.from_epsg(2230)  # US survey foot
        _, _, threshold = self._prep(cut_crs=target)
        m2 = const.DEFAULT_AREA_THRESHOLD_M2("h3", 5)
        factor = target.axis_info[0].unit_conversion_factor
        self.assertAlmostEqual(threshold, m2 / factor**2, delta=m2 * 0.001)

    def test_default_threshold_in_degree_crs(self):
        _, _, threshold = self._prep()
        m2 = const.DEFAULT_AREA_THRESHOLD_M2("h3", 5)
        metres_per_degree = 111_195  # pi/180 * mean earth radius
        expected = m2 / metres_per_degree**2
        self.assertAlmostEqual(threshold, expected, delta=expected * 0.001)
