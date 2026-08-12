from unittest import TestCase

import geopandas as gpd
import h3 as h3lib
import pyarrow.parquet as pq
from shapely.geometry import Polygon

from vector2dggs.common import (
    _clean_geometries,
    _prepare_dataframe,
    _run_bisection,
    bisection_preparation,
)
from vector2dggs.h3 import h3
from vector2dggs.indexers.h3vectorindexer import H3VectorIndexer
from vector2dggs.indexers.rhpvectorindexer import RHPVectorIndexer
from vector2dggs.rHP import rhp

from ..data.datapaths import (
    TEST_ANTIMERIDIAN_FILE_PATH,
    TEST_ANTIMERIDIAN_LAYER_NAME,
    TEST_OUTPUT_PATH,
)
from .base import TestRunthrough


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
        cleaned = self._cleaned_via_full_pipeline(RHPVectorIndexer("rhp"))

        self.assertEqual(len(cleaned), 2)
        self.assertTrue((cleaned.geometry.geom_type == "Polygon").all())
        # The true area of a 200km x 200km square near the equator is
        # roughly 3.2 degrees^2; the unfixed reprojection artifact wraps the
        # "long way" around the globe instead, giving an area in the
        # hundreds.
        self.assertAlmostEqual(cleaned.geometry.area.sum(), 3.25, places=1)

    def test_h3_run_across_antimeridian(self):
        """H3 doesn't require the pre-split fix - it indexes this correctly
        on its own."""
        h3(
            [
                TEST_ANTIMERIDIAN_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_ANTIMERIDIAN_LAYER_NAME,
                "-r",
                "4",
            ],
            standalone_mode=False,
        )
        files = sorted(TEST_OUTPUT_PATH.rglob("*.parquet"))
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
        rhp(
            [
                TEST_ANTIMERIDIAN_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_ANTIMERIDIAN_LAYER_NAME,
                "-r",
                "4",
            ],
            standalone_mode=False,
        )
        files = sorted(TEST_OUTPUT_PATH.rglob("*.parquet"))
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
    index rather than silently producing zero cells.
    """

    def _clean(self, poly):
        df = gpd.GeoDataFrame({"geometry": [poly]}, crs=4167)
        return _clean_geometries(df, H3VectorIndexer("h3"))

    def test_wholly_beyond_180_is_normalised_and_indexed(self):
        chatham_like = Polygon(
            [(183.1, -44.1), (183.8, -44.1), (183.8, -43.7), (183.1, -43.7)]
        )
        cleaned = self._clean(chatham_like)
        self.assertLessEqual(cleaned.total_bounds[2], 180)
        cells = H3VectorIndexer("h3").polyfill(cleaned, 7)
        self.assertGreater(len(cells), 0)

    def test_straddling_180_unwrapped_is_indexed_on_both_sides(self):
        straddler = Polygon(
            [(179.5, -44.0), (180.5, -44.0), (180.5, -43.5), (179.5, -43.5)]
        )
        cleaned = self._clean(straddler)
        cells = H3VectorIndexer("h3").polyfill(cleaned, 7)
        lngs = [h3lib.cell_to_latlng(c)[1] for c in cells.index]
        self.assertTrue(any(lng > 179 for lng in lngs))
        self.assertTrue(any(lng < -179 for lng in lngs))
