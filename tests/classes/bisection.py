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

    def test_default_threshold_scales_with_target_resolution(self):
        # granularity follows the target resolution (K cells per piece),
        # not the parent resolution
        target = pyproj.CRS.from_epsg(2193)
        df = gpd.GeoDataFrame({"geometry": [self.SQUARE_4167]}, crs=4167)
        _, _, threshold = common.bisection_preparation(df, "h3", 9, target, None)
        expected = const.DEFAULT_CUT_CELLS_PER_PIECE * const.DGGS_CELL_AREA_M2_BY_RES[
            "h3"
        ](9)
        self.assertAlmostEqual(threshold, expected, delta=expected * 0.001)

    def test_cut_crs_applies_without_explicit_threshold(self):
        target = pyproj.CRS.from_epsg(2193)
        df, cut_crs, threshold = self._prep(cut_crs=target)
        self.assertEqual(df.crs, target)
        self.assertEqual(cut_crs, target)
        # metre CRS: threshold is the default area in m^2, unconverted
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


class TestDroppedFeatureReport(TestCase):
    def test_features_with_no_cells_are_reported(self):
        import tempfile
        import warnings as _warnings

        big = Polygon([(174.7, -41.3), (174.9, -41.3), (174.9, -41.2), (174.7, -41.2)])
        tiny = Polygon(  # ~1 m^2: produces no cells at a coarse resolution
            [
                (174.75, -41.30),
                (174.75001, -41.30),
                (174.75001, -41.30001),
                (174.75, -41.30001),
            ]
        )
        df = gpd.GeoDataFrame(
            {"name": ["big", "tiny"], "geometry": [big, tiny]}, crs=4326
        )
        with tempfile.TemporaryDirectory() as d:
            with _warnings.catch_warnings():
                _warnings.simplefilter("ignore")
                df.to_file(f"{d}/in.gpkg", layer="x")
            with self.assertLogs(common.LOGGER, level="WARNING") as logs:
                common.index(
                    "h3",
                    f"{d}/in.gpkg",
                    f"{d}/out.pq",
                    7,
                    None,
                    False,
                    50,
                    0.0,
                    1,
                    layer="x",
                    compact=False,
                )
        self.assertTrue(
            any("1 of 2 features produced no cells" in m for m in logs.output),
            logs.output,
        )


class TestGeodesicCutEdges(TestCase):
    """
    Adjacent bisection pieces can carry different vertices along the same cut
    line (T-junctions from cuts at different recursion depths). Geodesic
    backends read each chord as a great circle, so the two curves differ and
    a cell whose centre falls between them belongs to neither piece.
    Coordinates from a real S2 r15 reproducer: two chords of the same
    constant-latitude cut line, whose great circles bulge 0.39m and 0.20m
    poleward; the missing cell's centre sits 0.29m poleward, in the gap.
    """

    CUT_LAT = -41.857542761228
    MISSING_CELL = "6d3a4841c"
    EPS_DEG = 0.0002838765651889054  # 31.6m: cell edge at r15 / 10

    def _pieces(self):
        south = Polygon(
            [
                (173.1988782522566, -41.88),
                (173.2559078324737, -41.88),
                (173.2559078324737, self.CUT_LAT),
                (173.1988782522566, self.CUT_LAT),
            ]
        )
        north = Polygon(
            [
                (173.2063719775, self.CUT_LAT),
                (173.247464236, self.CUT_LAT),
                (173.247464236, -41.84),
                (173.2063719775, -41.84),
            ]
        )
        return south, north

    def test_eps_vertex_spacing_closes_t_junction_gap(self):
        from .base import skip_unless_backend

        skip_unless_backend("s2")
        import shapely

        from vector2dggs.indexerfactory import indexer_instance

        indexer = indexer_instance("s2")

        def union_tokens(geoms):
            tokens: set = set()
            for geom in geoms:
                tokens |= indexer.tokens_from_polygon(geom, 15)
            return tokens

        sparse = self._pieces()
        self.assertNotIn(self.MISSING_CELL, union_tokens(sparse))
        dense = (shapely.segmentize(g, self.EPS_DEG) for g in sparse)
        self.assertIn(self.MISSING_CELL, union_tokens(dense))

    def test_bisection_caps_cut_edge_spacing(self):
        # axis-aligned square: any piece boundary segment not on the outer
        # bbox is a cut segment, and must respect the blade_segment cap
        square = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
        eps = 0.01
        pieces = [
            g
            for g in common.bisect_geometry(square, 0.05, eps).geoms
            if g.geom_type == "Polygon"
        ]
        self.assertGreater(len(pieces), 2)
        for piece in pieces:
            coords = list(piece.exterior.coords)
            for (x0, y0), (x1, y1) in zip(coords, coords[1:], strict=False):
                on_bbox = (y0 == y1 and y0 in (0.0, 1.0)) or (
                    x0 == x1 and x0 in (0.0, 1.0)
                )
                if not on_bbox:
                    length = ((x1 - x0) ** 2 + (y1 - y0) ** 2) ** 0.5
                    self.assertLessEqual(length, eps * 1.001)
