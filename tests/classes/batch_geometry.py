from unittest import TestCase

import geopandas as gpd
from shapely.geometry import Point

from vector2dggs.indexerfactory import indexer_instance

from .base import skip_unless_backend


class TestBatchCellGeometry(TestCase):
    """
    cells_to_points / cells_to_polygons must agree with the per-cell scalar
    methods, across rHP cell shapes (quad, dart, cap) and mixed resolutions
    (the compaction merge hands them a mixed-resolution index).
    """

    # caps (N4), darts (N0, S2), quads, at resolutions 0 through 5
    RHP_CELLS = ["S", "N0", "N4", "S2", "P44", "N012", "Q333", "R88735"]

    def test_rhp_batch_polygons_match_scalar(self):
        skip_unless_backend("rhp")
        idx = indexer_instance("rhp")
        batch = list(idx.cells_to_polygons(self.RHP_CELLS))
        self.assertEqual(len(batch), len(self.RHP_CELLS))
        for cell, poly in zip(self.RHP_CELLS, batch, strict=True):
            self.assertTrue(
                poly.equals_exact(idx.cell_to_polygon(cell), tolerance=1e-9),
                f"boundary mismatch for {cell}",
            )

    def test_rhp_batch_points_match_scalar(self):
        skip_unless_backend("rhp")
        idx = indexer_instance("rhp")
        batch = list(idx.cells_to_points(self.RHP_CELLS))
        for cell, point in zip(self.RHP_CELLS, batch, strict=True):
            self.assertTrue(
                point.equals_exact(idx.cell_to_point(cell), tolerance=1e-9),
                f"centroid mismatch for {cell}",
            )

    def test_default_batch_methods_match_scalar(self):
        skip_unless_backend("h3")
        import h3

        idx = indexer_instance("h3")
        cells = [h3.latlng_to_cell(-41.3, 174.8, r) for r in (5, 9)]
        self.assertEqual(
            list(idx.cells_to_polygons(cells)),
            [idx.cell_to_polygon(c) for c in cells],
        )
        self.assertEqual(
            list(idx.cells_to_points(cells)),
            [idx.cell_to_point(c) for c in cells],
        )


class TestBatchPointIndexing(TestCase):
    """rHP point indexing over a whole frame must equal per-point lookup."""

    POINTS = [Point(174.8, -41.3), Point(0.1, 0.2), Point(-176.3, -43.7)]

    def test_rhp_points_batch_matches_scalar(self):
        skip_unless_backend("rhp")
        from rhealpixdggs.rhp_wrappers import geo_to_rhp

        idx = indexer_instance("rhp")
        df = gpd.GeoDataFrame(
            {"fid": range(len(self.POINTS)), "geometry": self.POINTS}, crs=4326
        )
        out = idx._polyfill_points(df.copy(), 7)
        expected = {
            (geo_to_rhp(p.y, p.x, 7, plane=False), i) for i, p in enumerate(self.POINTS)
        }
        self.assertEqual(set(zip(out.index, out["fid"], strict=True)), expected)
        self.assertIsNone(out.index.name)
        self.assertNotIn("geometry", out.columns)
