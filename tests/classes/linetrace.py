from unittest import TestCase, mock

import geopandas as gpd
from shapely.geometry import LineString

from vector2dggs.indexerfactory import indexer_instance


class TestLinetraceSetSemantics(TestCase):
    """
    Linestring output contract: one row per (feature, cell), for every
    backend. A cell shared by two features must appear once for each, and a
    feature re-entering a cell must not produce duplicate rows.
    """

    RESOLUTIONS = {"h3": 9, "rhp": 8, "s2": 14, "a5": 14, "geohash": 6}

    # Crossing guarantees a shared cell at any resolution
    LINE_A = LineString([(174.75, -41.30), (174.85, -41.30)])
    LINE_B = LineString([(174.80, -41.25), (174.80, -41.35)])
    # Closes back onto its start point, guaranteeing cell re-entry
    ZIGZAG = LineString(
        [
            (174.80, -41.30),
            (174.85, -41.31),
            (174.80, -41.32),
            (174.85, -41.33),
            (174.80, -41.30),
        ]
    )

    def _backends(self):
        for dggs, res in self.RESOLUTIONS.items():
            try:
                yield indexer_instance(dggs), res
            except ImportError:
                continue

    @staticmethod
    def _polyfill(indexer, geoms, res):
        df = gpd.GeoDataFrame(
            {"fid": list(range(len(geoms))), "geometry": geoms}, crs=4326
        )
        return indexer.polyfill(df, res)

    def test_batch_matches_solo(self):
        for indexer, res in self._backends():
            with self.subTest(dggs=indexer.dggs):
                batch = self._polyfill(indexer, [self.LINE_A, self.LINE_B], res)
                for fid, line in enumerate([self.LINE_A, self.LINE_B]):
                    solo = self._polyfill(indexer, [line], res)
                    batch_cells = set(batch[batch["fid"] == fid].index)
                    self.assertEqual(batch_cells, set(solo.index))

    def test_no_duplicate_feature_cell_pairs(self):
        for indexer, res in self._backends():
            with self.subTest(dggs=indexer.dggs):
                result = self._polyfill(indexer, [self.ZIGZAG, self.LINE_A], res)
                pairs = list(zip(result.index, result["fid"], strict=True))
                self.assertEqual(len(pairs), len(set(pairs)))


class TestEmptyTraces(TestCase):
    """
    A linestring whose trace yields no cells must contribute no rows - not a
    NaN-indexed row (which crashes S2's secondary_index and silently
    propagates into rHP output).
    """

    LINE = LineString([(174.75, -41.30), (174.85, -41.30)])

    def _assert_no_nan_rows(self, indexer, result, n_expected_features):
        self.assertTrue(result.index.notna().all(), "NaN cell rows leaked")
        self.assertEqual(result["fid"].nunique(), n_expected_features)

    def test_rhp_empty_trace_is_dropped(self):
        try:
            indexer = indexer_instance("rhp")
        except ImportError:
            self.skipTest("rhp backend not installed")
        df = gpd.GeoDataFrame(
            {"fid": [0, 1], "geometry": [self.LINE, self.LINE]}, crs=4326
        )
        real = type(indexer)._linetrace
        calls = iter([True, False])

        def fake(geom, resolution):
            return [] if next(calls) else real(geom, resolution)

        with mock.patch.object(type(indexer), "_linetrace", staticmethod(fake)):
            result = indexer._polyfill_linestrings(df, 6)
        self._assert_no_nan_rows(indexer, result, 1)

    def test_s2_empty_trace_is_dropped(self):
        try:
            indexer = indexer_instance("s2")
        except ImportError:
            self.skipTest("s2 backend not installed")
        df = gpd.GeoDataFrame(
            {"fid": [0, 1], "geometry": [self.LINE, self.LINE]}, crs=4326
        )
        real = type(indexer).cells_from_linestring
        calls = iter([True, False])

        def fake(self_idx, geom, level):
            return [] if next(calls) else real(self_idx, geom, level)

        with mock.patch.object(type(indexer), "cells_from_linestring", fake):
            result = indexer._polyfill_linestrings(df, 14)
        self._assert_no_nan_rows(indexer, result, 1)

    def test_s2_subcell_polygon_is_dropped(self):
        try:
            indexer = indexer_instance("s2")
        except ImportError:
            self.skipTest("s2 backend not installed")
        from shapely.geometry import Polygon

        tiny = Polygon(  # ~1 m^2: no level-10 cell centre can fall inside
            [
                (174.75, -41.30),
                (174.75001, -41.30),
                (174.75001, -41.30001),
                (174.75, -41.30001),
            ]
        )
        big = Polygon([(174.7, -41.3), (174.9, -41.3), (174.9, -41.2), (174.7, -41.2)])
        df = gpd.GeoDataFrame({"fid": [0, 1], "geometry": [tiny, big]}, crs=4326)
        result = indexer._polyfill_polygons(df, 10)
        self._assert_no_nan_rows(indexer, result, 1)

    def test_geohash_subcell_polygon_is_dropped(self):
        try:
            indexer = indexer_instance("geohash")
        except ImportError:
            self.skipTest("geohash backend not installed")
        from shapely.geometry import Polygon

        tiny = Polygon(  # ~1 m^2: no level-6 cell centre can fall inside
            [
                (174.75, -41.30),
                (174.75001, -41.30),
                (174.75001, -41.30001),
                (174.75, -41.30001),
            ]
        )
        big = Polygon([(174.7, -41.3), (174.9, -41.3), (174.9, -41.2), (174.7, -41.2)])
        df = gpd.GeoDataFrame({"fid": [0, 1], "geometry": [tiny, big]}, crs=4326)
        result = indexer._polyfill_polygons(df, 6)
        self._assert_no_nan_rows(indexer, result, 1)
