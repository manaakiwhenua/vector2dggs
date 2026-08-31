import io
from unittest import TestCase

import geopandas as gpd
import pyproj
from shapely.geometry import LineString, Polygon
from tqdm import tqdm

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


class TestSharedBisectionProgress(TestCase):
    """
    A pbar passed into _run_bisection is shared across calls: its total
    grows and updates accumulate onto the same bar.
    """

    SQUARE = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])

    @staticmethod
    def _silent_pbar():
        # disable=True would make update() a no-op; redirect output instead.
        return tqdm(total=0, file=io.StringIO())

    def test_shared_pbar_accumulates_total_and_count_across_calls(self):
        df1 = gpd.GeoDataFrame({"geometry": [self.SQUARE]}, crs=2193)
        df2 = gpd.GeoDataFrame({"geometry": [self.SQUARE, self.SQUARE]}, crs=2193)
        pbar = self._silent_pbar()

        _run_bisection(df1.copy(), 5.0, 1, pbar=pbar)
        self.assertEqual(pbar.total, 1)
        self.assertEqual(pbar.n, 1)

        _run_bisection(df2.copy(), 5.0, 1, pbar=pbar)
        self.assertEqual(pbar.total, 3)
        self.assertEqual(pbar.n, 3)
        pbar.close()

    def test_caller_retains_ownership_of_shared_pbar(self):
        # close() sets disable=True; staying False confirms the shared bar
        # wasn't closed by _run_bisection itself.
        df = gpd.GeoDataFrame({"geometry": [self.SQUARE]}, crs=2193)
        pbar = self._silent_pbar()
        _run_bisection(df.copy(), 5.0, 1, pbar=pbar)
        self.assertFalse(pbar.disable)
        pbar.close()

    def test_no_pbar_given_falls_back_to_per_call_bar(self):
        df = gpd.GeoDataFrame({"geometry": [self.SQUARE]}, crs=2193)
        result = _run_bisection(df.copy(), 5.0, 1)
        self.assertAlmostEqual(result.geometry.iloc[0].area, self.SQUARE.area, places=6)


class TestBisectionPreparation(TestCase):
    """
    Default cut_threshold derivation must be exact for whatever CRS unit
    the input data arrives in (via pyproj unit conversion factors).
    """

    SQUARE_4167 = Polygon([(174, -41), (174.1, -41), (174.1, -40.9), (174, -40.9)])

    def _threshold(self, crs=None, resolution=5):
        df = gpd.GeoDataFrame({"geometry": [self.SQUARE_4167]}, crs=4167)
        if crs is not None:
            df = df.to_crs(crs)
        return common._derive_cut_threshold(df, "h3", resolution)

    def test_default_threshold_scales_with_target_resolution(self):
        # granularity follows the target resolution (K cells per piece),
        # not the parent resolution
        threshold = self._threshold(crs=2193, resolution=9)
        expected = const.DEFAULT_CUT_CELLS_PER_PIECE * const.DGGS_CELL_AREA_M2_BY_RES[
            "h3"
        ](9)
        self.assertAlmostEqual(threshold, expected, delta=expected * 0.001)

    def test_default_threshold_in_metre_crs(self):
        # metre CRS: threshold is the default area in m^2, unconverted
        threshold = self._threshold(crs=2193)
        self.assertAlmostEqual(
            threshold, const.DEFAULT_AREA_THRESHOLD_M2("h3", 5), delta=1
        )

    def test_default_threshold_in_foot_crs(self):
        target = pyproj.CRS.from_epsg(2230)  # US survey foot
        threshold = self._threshold(crs=target)
        m2 = const.DEFAULT_AREA_THRESHOLD_M2("h3", 5)
        factor = target.axis_info[0].unit_conversion_factor
        self.assertAlmostEqual(threshold, m2 / factor**2, delta=m2 * 0.001)

    def test_default_threshold_in_degree_crs(self):
        threshold = self._threshold()
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
        import s2geometry as S2
        import shapely

        from vector2dggs.indexerfactory import indexer_instance

        indexer = indexer_instance("s2")
        # cells_from_polygon returns the native uint64 form; MISSING_CELL is
        # recorded as a token above for readability, so convert it once here.
        missing_cell = S2.S2CellId.FromToken(self.MISSING_CELL).id()

        def union_cells(geoms):
            cells: set = set()
            for geom in geoms:
                cells |= indexer.cells_from_polygon(geom, 15)
            return cells

        sparse = self._pieces()
        self.assertNotIn(missing_cell, union_cells(sparse))
        dense = (shapely.segmentize(g, self.EPS_DEG) for g in sparse)
        self.assertIn(missing_cell, union_cells(dense))

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


class TestLinestringBisection(TestCase):
    """
    Long linestrings are split at existing vertices when cumulative arc
    length exceeds the derived budget. Vertex-only cuts leave every
    vertex-to-vertex segment untouched, so cell output is identical to the
    uncut trace for every backend.
    """

    def _long_line(self, n=400, step=0.02):
        # ~880 km of jittered line at ~-41S, many vertices
        coords = [
            (172.0 + i * step, -41.0 + (0.005 if i % 2 else -0.005)) for i in range(n)
        ]
        return LineString(coords)

    def test_long_line_is_split_at_vertices(self):
        line = self._long_line()
        df = gpd.GeoDataFrame({"geometry": [line]}, crs=4326)
        df.index.name = "fid"
        out = _run_bisection(
            df.copy(), 1e12, 1, line_budget=1.0
        )  # budget: 1 degree-ish
        pieces = list(out.geometry.iloc[0].geoms)
        self.assertGreater(len(pieces), 3)
        # every piece boundary vertex is an original vertex
        original = set(line.coords)
        for p in pieces:
            self.assertIn(p.coords[0], original)
            self.assertIn(p.coords[-1], original)
        # pieces chain exactly through the original coordinate sequence
        rebuilt = list(pieces[0].coords)
        for p in pieces[1:]:
            self.assertEqual(rebuilt[-1], p.coords[0])
            rebuilt.extend(list(p.coords)[1:])
        self.assertEqual(rebuilt, list(line.coords))

    def test_split_output_cells_identical(self):
        from .base import skip_unless_backend

        skip_unless_backend("h3")
        import tempfile
        import warnings as _warnings

        import pandas as pd

        line = self._long_line()
        df = gpd.GeoDataFrame({"name": ["x"], "geometry": [line]}, crs=4326)
        with tempfile.TemporaryDirectory() as d:
            with _warnings.catch_warnings():
                _warnings.simplefilter("ignore")
                df.to_file(f"{d}/in.gpkg", layer="l")
            results = []
            for cut_threshold in (None, 0.0):  # default (auto line split) vs none
                common.index(
                    "h3",
                    f"{d}/in.gpkg",
                    f"{d}/out{cut_threshold}.pq",
                    7,
                    None,
                    False,
                    cut_threshold,
                    1,
                    layer="l",
                    compact=False,
                )
                out = pd.read_parquet(f"{d}/out{cut_threshold}.pq")
                results.append(set(out.index))
        self.assertGreater(len(results[0]), 0)
        self.assertEqual(results[0], results[1])
