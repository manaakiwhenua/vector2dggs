import time
import warnings
from unittest import TestCase, mock

import geopandas as gpd
from shapely.geometry import Polygon, box

import vector2dggs.constants as const

from ..data.datapaths import TEST_DISPERSED_FILE_PATH, TEST_DISPERSED_LAYER_NAME
from .base import skip_unless_backend

try:
    from rhealpixdggs.dggs import WGS84_003
    from rhealpixdggs.rhp_wrappers import polyfill_array

    from vector2dggs.indexers.rhpvectorindexer import RHPVectorIndexer
except ImportError:
    RHPVectorIndexer = None


class TestRHPFillMinRes(TestCase):
    """
    Unit tests for RHPVectorIndexer._fill_min_res: the resolution polyfill's
    hierarchical descent should start from for a given geometry, chosen from
    the geometry's own bounding-box extent (issue: rhealpixdggs 0.8.5's
    hierarchical polyfill pays a roughly fixed cost per level descended from
    min_res, so starting from 0 for every geometry - the library default -
    is catastrophic for the small polygons real vector data is mostly made
    of, even though it's a big win for large/complex ones).
    """

    def setUp(self):
        skip_unless_backend("rhp")

    def test_picks_finest_resolution_containing_bbox(self):
        # ~1km box at the equator: finest resolution whose cell is still at
        # least as wide as the bbox, but no finer.
        geom = box(0.0, 0.0, 0.009, 0.009)  # ~1km x 1km at the equator
        min_res = RHPVectorIndexer._fill_min_res(geom, 12)
        span_m = 0.009 * const.METRES_PER_DEGREE
        cell_width = const.DGGS_CELL_AREA_M2_BY_RES["rhp"](min_res) ** 0.5
        self.assertGreaterEqual(cell_width, span_m)
        if min_res < const.MAX_RHP:
            next_width = const.DGGS_CELL_AREA_M2_BY_RES["rhp"](min_res + 1) ** 0.5
            self.assertLess(next_width, span_m)

    def test_within_upstream_bounds(self):
        # polyfill_array raises ValueError outside [0, resolution]
        geoms = [
            box(0, 0, 0.001, 0.001),
            box(-10, -10, 10, 10),
            box(170, -45, 175, -40),
            Polygon([(0, 0), (0.0001, 0), (0.0001, 0.0001), (0, 0.0001)]),
        ]
        for geom in geoms:
            for resolution in (0, 1, 5, 9, const.MAX_RHP):
                with self.subTest(geom=geom.wkt, resolution=resolution):
                    min_res = RHPVectorIndexer._fill_min_res(geom, resolution)
                    self.assertGreaterEqual(min_res, 0)
                    self.assertLessEqual(min_res, resolution)

    def test_pole_or_global_geometry_starts_at_zero(self):
        """
        A bbox reaching a pole or spanning every longitude gets the whole
        polar square as its candidate region (RHEALPixDGGS._candidate_boxes)
        regardless of how small the geometry actually is: a tiny
        pole-touching bbox's min_res=8 lattice was confirmed to be ~42
        million cells. A bbox-diagonal heuristic alone would pick a *deep*
        min_res for such a tiny geometry and hang - this must return 0
        unconditionally for these two cases. Deliberately a correctness/unit
        test, not a timing test: unguarded, this hangs rather than merely
        runs slow, so a timing budget would not catch a regression here
        within any reasonable test suite run.
        """
        north_pole = box(0.0, 89.99, 0.01, 90.0)
        south_pole = box(0.0, -90.0, 0.01, -89.99)
        antimeridian_spanning = box(-180.0, -10.0, 180.0, 10.0)
        for geom in (north_pole, south_pole, antimeridian_spanning):
            with self.subTest(geom=geom.wkt):
                self.assertEqual(RHPVectorIndexer._fill_min_res(geom, 9), 0)

    def test_near_pole_but_not_touching_is_unaffected(self):
        # Confirms the clamp is on the boundary condition, not "near a pole"
        # generally - a geometry that merely approaches a pole should still
        # get a deep min_res like any other small geometry.
        near_pole = box(0.0, 89.97, 0.01, 89.98)
        min_res = RHPVectorIndexer._fill_min_res(near_pole, 9)
        self.assertGreater(min_res, 0)

    def test_large_geometry_starts_coarse(self):
        large = box(160.0, -47.0, 179.0, -34.0)  # ~13 degrees, NZ-scale
        min_res = RHPVectorIndexer._fill_min_res(large, 9)
        self.assertLessEqual(min_res, 1)


class TestRHPFillEquivalence(TestCase):
    """
    Correctness lock-in: RHPVectorIndexer._polyfill_polygon (which picks a
    per-geometry min_res) must produce exactly the same cells as calling
    polyfill_array directly with min_res=0 (the library default) and with
    min_res=resolution (a cell-by-cell test of every candidate cell at the
    target resolution - the semantics of the pre-0.8.5 algorithm). This
    proves both correctness against the old behaviour and min_res
    invariance in the same assertion.
    """

    def setUp(self):
        skip_unless_backend("rhp")
        self.indexer = RHPVectorIndexer(dggs="rhp")

    def _assert_min_res_invariant(self, geom, resolution):
        got = set(self.indexer._polyfill_polygon(geom, resolution))
        for min_res in (0, resolution):
            arr = polyfill_array(
                geom, resolution, plane=False, dggs=WGS84_003, min_res=min_res
            )
            want = set() if arr is None else set(arr.tolist())
            self.assertEqual(got, want, f"mismatch at min_res={min_res} for {geom.wkt}")

    def test_small_polygon(self):
        geom = box(174.7, -41.30, 174.701, -41.299)
        self._assert_min_res_invariant(geom, 10)

    def test_large_polygon(self):
        geom = box(160.0, -47.0, 179.0, -34.0)
        self._assert_min_res_invariant(geom, 5)

    def test_donut_hole(self):
        shell = [(174.70, -41.30), (174.76, -41.30), (174.76, -41.24), (174.70, -41.24)]
        hole = [(174.72, -41.28), (174.74, -41.28), (174.74, -41.26), (174.72, -41.26)]
        donut = Polygon(shell, [hole])
        self._assert_min_res_invariant(donut, 9)

    def test_cells_are_sorted(self):
        # polyfill_array's output is documented-sorted; _polyfill_polygon
        # should pass that straight through rather than re-set()-ing it
        # (which would make row order depend on Python's hash
        # randomisation across runs).
        geom = box(174.7, -41.30, 174.9, -41.20)
        cells = self.indexer._polyfill_polygon(geom, 8)
        self.assertGreater(len(cells), 1)
        self.assertEqual(cells, sorted(cells))


class TestRHPFillPerformance(TestCase):
    """
    Regression guard for the min_res fix: without it, rhealpixdggs 0.8.5's
    hierarchical polyfill regresses ~900x on typical small real-world
    polygons (measured: 345s -> 1.05s for tests/data/chathams-pannz-2014.gpkg
    at resolution 9). Two layers: a deterministic check that the argument is
    actually being passed (catches a future refactor dropping it, with zero
    timing flakiness), and generous wall-clock budgets (CI runs pytest -n
    auto on modest runners) that still catch a regression of that magnitude.
    """

    def setUp(self):
        skip_unless_backend("rhp")
        self.indexer = RHPVectorIndexer(dggs="rhp")

    def test_min_res_is_passed_to_polyfill(self):
        geom = box(174.7, -41.30, 174.701, -41.299)
        resolution = 10
        expected = RHPVectorIndexer._fill_min_res(geom, resolution)
        self.assertGreater(expected, 0)
        with mock.patch(
            "vector2dggs.indexers.rhpvectorindexer.polyfill_array",
            wraps=polyfill_array,
        ) as mocked:
            RHPVectorIndexer._polyfill_polygon(geom, resolution)
        self.assertEqual(mocked.call_args.kwargs["min_res"], expected)

    def test_small_polygon_fill_is_fast(self):
        geom = box(174.7, -41.30, 174.701, -41.299)
        t0 = time.perf_counter()
        self.indexer._polyfill_polygon(geom, 9)
        elapsed = time.perf_counter() - t0
        # measured healthy cost ~0.01s; unguarded (min_res=0) cost ~10.8s
        # for a comparably small/complex polygon - generous headroom either
        # way of that gap.
        self.assertLess(elapsed, 3.0, f"took {elapsed:.2f}s - min_res fix regressed?")

    def test_dispersed_fixture_fill_is_fast(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            gdf = gpd.read_file(
                TEST_DISPERSED_FILE_PATH, layer=TEST_DISPERSED_LAYER_NAME
            ).to_crs(4326)
        gdf = gdf.explode(index_parts=False)
        gdf = gdf[gdf.geometry.geom_type.isin(("Polygon",))]
        self.assertGreater(len(gdf), 0)
        t0 = time.perf_counter()
        for geom in gdf.geometry:
            self.indexer._polyfill_polygon(geom, 9)
        elapsed = time.perf_counter() - t0
        # measured healthy cost ~0.24s; unguarded (min_res=0) cost ~345s for
        # this exact fixture at resolution 8 - a 30s budget is ~100x
        # headroom above healthy and still well below the pathological cost.
        self.assertLess(elapsed, 30.0, f"took {elapsed:.2f}s - min_res fix regressed?")
