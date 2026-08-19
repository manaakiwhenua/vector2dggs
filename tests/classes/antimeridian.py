import warnings
from unittest import TestCase

import geopandas as gpd
import pyarrow.parquet as pq
from shapely.geometry import Polygon

from vector2dggs.common import (
    _clean_geometries,
    _normalise_longitudes,
    _prepare_dataframe,
    _run_bisection,
    bisection_preparation,
)
from vector2dggs.h3 import h3
from vector2dggs.indexerfactory import indexer_instance
from vector2dggs.rHP import rhp

from ..data.datapaths import (
    TEST_ANTIMERIDIAN_FILE_PATH,
    TEST_ANTIMERIDIAN_LAYER_NAME,
)
from .base import TestRunthrough, skip_unless_backend


class TestAntimeridian(TestRunthrough):
    """
    tests/data/antimeridian.gpkg's "antimeridian_strip" layer is a 200km x
    200km square defined in a projected CRS (azimuthal equidistant, centred
    exactly on the antimeridian at the equator) so the source coordinates are
    continuous and unambiguous - there is exactly one correct interpretation
    of this geometry, unlike a raw WGS84 polygon with vertices near +/-180
    degrees longitude, which is inherently ambiguous.

    H3, S2 and A5 perform their own geodesic point-in-polygon containment
    tests and index this correctly with or without pre-splitting (see
    VectorIndexer.GEODESIC_POLYFILL); rHEALPix and geohash do not, and rely
    on common._clean_geometries pre-splitting it for them.
    """

    def _cleaned_via_full_pipeline(self, indexer):
        """Runs the fixture through the same stages index() does before
        indexing: bisection (which wraps every geometry in a
        GeometryCollection whenever cut_threshold is set, i.e. by default)
        followed by _clean_geometries."""
        gdf = gpd.read_file(
            TEST_ANTIMERIDIAN_FILE_PATH, layer=TEST_ANTIMERIDIAN_LAYER_NAME
        )
        gdf = _prepare_dataframe(gdf, None, False)
        df, _cut_crs, cut_threshold = bisection_preparation(
            gdf, indexer.dggs, 0, None, None
        )
        df = _run_bisection(df, cut_threshold, 1)
        return _clean_geometries(df, indexer)

    def test_clean_geometries_splits_for_backends_that_require_it(self):
        skip_unless_backend("rhp")
        cleaned = self._cleaned_via_full_pipeline(indexer_instance("rhp"))

        self.assertEqual(len(cleaned), 2)
        self.assertTrue((cleaned.geometry.geom_type == "Polygon").all())
        # The true area of a 200km x 200km square near the equator is
        # roughly 3.2 degrees^2; the unfixed reprojection artifact wraps the
        # "long way" around the globe instead, giving an area in the
        # hundreds. Square degrees are the point here, so silence the
        # geographic-CRS area warning.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            area = cleaned.geometry.area.sum()
        self.assertAlmostEqual(area, 3.25, places=1)

    def test_h3_run_across_antimeridian(self):
        """H3 doesn't require the pre-split fix - it indexes this correctly
        on its own."""
        skip_unless_backend("h3")
        h3(
            [
                TEST_ANTIMERIDIAN_FILE_PATH,
                str(self.output_path),
                "--layer",
                TEST_ANTIMERIDIAN_LAYER_NAME,
                "-r",
                "4",
                "-t",
                "1",
            ],
            standalone_mode=False,
        )
        files = sorted(self.output_path.rglob("*.parquet"))
        self.assertTrue(files, "No parquet files written")

        cell_ids = set()
        for f in files:
            table = pq.read_table(f)
            cell_ids.update(table.column("h3_04").to_pylist())

        # A correctly-indexed 200km x 200km square should touch a modest,
        # bounded number of H3 cells, on both sides of the antimeridian.
        self.assertLess(len(cell_ids), 1000)
        self.assertGreater(len(cell_ids), 0)

    def test_rhp_run_across_antimeridian(self):
        """rHEALPix requires the pre-split fix; without it, the unfixed
        reprojection artifact indexes as ~322 cells instead of ~2."""
        skip_unless_backend("rhp")
        rhp(
            [
                TEST_ANTIMERIDIAN_FILE_PATH,
                str(self.output_path),
                "--layer",
                TEST_ANTIMERIDIAN_LAYER_NAME,
                "-r",
                "4",
                "-t",
                "1",
            ],
            standalone_mode=False,
        )
        files = sorted(self.output_path.rglob("*.parquet"))
        self.assertTrue(files, "No parquet files written")

        cell_ids = set()
        for f in files:
            table = pq.read_table(f)
            cell_ids.update(table.column("rhp_04").to_pylist())

        self.assertLess(len(cell_ids), 50)
        self.assertGreater(len(cell_ids), 0)


class TestUnwrappedLongitudes(TestCase):
    """
    Geographic input may store longitudes beyond +/-180 (e.g. the Chatham
    Islands at 183 degrees E in EPSG:4167). These must be normalised so they
    index rather than silently producing zero cells. Run against every
    installed backend: the straddling case exercises each backend's
    antimeridian handling (geodesic polyfill, or the pre-split fix).
    """

    RESOLUTIONS = {"h3": 7, "rhp": 7, "s2": 12, "a5": 12, "geohash": 5}

    def _backends(self):
        for dggs, res in self.RESOLUTIONS.items():
            try:
                yield indexer_instance(dggs), res
            except ImportError:
                continue

    def _cells(self, indexer, poly, res):
        df = gpd.GeoDataFrame({"geometry": [poly]}, crs=4167)
        cleaned = _clean_geometries(df, indexer)
        self.assertLessEqual(cleaned.total_bounds[2], 180)
        return indexer.polyfill(cleaned, res)

    def test_rejects_projected_crs(self):
        df = gpd.GeoDataFrame(
            {"geometry": [Polygon([(0, 0), (1000, 0), (1000, 1000), (0, 1000)])]},
            crs=2193,
        )
        with self.assertRaises(ValueError):
            _normalise_longitudes(df)

    def test_wholly_beyond_180_is_normalised_and_indexed(self):
        chatham_like = Polygon(
            [(183.1, -44.1), (183.8, -44.1), (183.8, -43.7), (183.1, -43.7)]
        )
        for indexer, res in self._backends():
            with self.subTest(dggs=indexer.dggs):
                cells = self._cells(indexer, chatham_like, res)
                self.assertGreater(len(cells), 0)
                lngs = [indexer.cell_to_point(c).x for c in cells.index]
                self.assertTrue(all(-178 < lng < -175 for lng in lngs))

    def test_straddling_180_unwrapped_is_indexed_on_both_sides(self):
        straddler = Polygon(
            [(179.5, -44.0), (180.5, -44.0), (180.5, -43.5), (179.5, -43.5)]
        )
        for indexer, res in self._backends():
            with self.subTest(dggs=indexer.dggs):
                cells = self._cells(indexer, straddler, res)
                lngs = [indexer.cell_to_point(c).x for c in cells.index]
                self.assertTrue(any(lng > 170 for lng in lngs))
                self.assertTrue(any(lng < -170 for lng in lngs))
