from unittest import TestCase

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
