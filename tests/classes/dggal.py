from unittest import TestCase

import geopandas as gpd
import pandas as pd
import shapely
from shapely.geometry import LineString, Polygon, box

from vector2dggs.indexerfactory import indexer_instance
from vector2dggs.indexers.vectorindexer import VectorIndexer

from .base import skip_unless_backend

# A box with a hole, several hundred cells across at each grid's RES.
REGION = Polygon(
    box(174.60, -41.40, 174.90, -41.15).exterior.coords,
    [box(174.72, -41.30, 174.78, -41.25).exterior.coords],
)
LINE = LineString([(174.55, -41.45), (174.70, -41.20), (174.95, -41.35)])
RES = {"isea9r": 9, "healpix": 14, "isea4r": 14}


class DGGALGridScenarios:
    """
    The contracts a DGGAL grid must meet, asserted against brute force over
    the zones of a bounding box with DGGAL's own refined outlines.
    """

    DGGS: str

    @classmethod
    def setUpClass(cls):
        skip_unless_backend(cls.DGGS)
        super().setUpClass()

    def setUp(self):
        self.res = RES[self.DGGS]
        self.centre = indexer_instance(self.DGGS)
        self.intersects = indexer_instance(self.DGGS, "intersects")
        self.dggrs = self.centre.dggrs

    def _bbox_zones(self, geom):
        return self.centre._bbox_zones(geom, self.res)

    def _refined(self, zones):
        return [Polygon(r) for r in self.centre._rings(zones, refined=True)]

    def test_centre_fill_is_the_zones_with_centroids_inside(self):
        got = set(self.centre._polyfill_polygon(REGION, self.res))
        zones = self._bbox_zones(REGION)
        pts = self.centre._centroids(zones)
        want = {
            z
            for z, (x, y) in zip(zones, pts, strict=True)
            if REGION.contains(shapely.Point(x, y))
        }
        self.assertEqual(got, want)
        self.assertGreater(len(got), 100)

    def test_intersects_fill_is_every_zone_the_region_meets(self):
        got = set(self.intersects._polyfill_polygon(REGION, self.res))
        zones = self._bbox_zones(REGION)
        want = {
            z
            for z, p in zip(zones, self._refined(zones), strict=True)
            if p.intersects(REGION)
        }
        self.assertEqual(got, want)
        self.assertTrue(set(self.centre._polyfill_polygon(REGION, self.res)) < got)

    def test_line_cover_is_every_zone_the_line_crosses(self):
        got = set(self.centre._linetrace(LINE, self.res))
        zones = self._bbox_zones(LINE.buffer(0.01))
        want = {
            z
            for z, p in zip(zones, self._refined(zones), strict=True)
            if p.intersects(LINE)
        }
        self.assertEqual(got, want)

    def test_points_land_in_the_zone_holding_them(self):
        df = gpd.GeoDataFrame(
            {"fid": [1, 2]},
            geometry=[shapely.Point(174.7, -41.3), shapely.Point(0.0, 0.0)],
            crs=4326,
        )
        out = self.centre._polyfill_points(df, self.res)
        for zone, pt in zip(out.index, df.geometry, strict=True):
            self.assertTrue(self.centre.cell_to_polygon(zone).covers(pt))

    def test_parent_partition_is_an_ancestor_at_parent_res(self):
        cells = self.centre._polyfill_polygon(REGION, self.res)[:50]
        df = pd.DataFrame({"fid": 1}, index=pd.Index(cells))
        parent_res = self.res - 3
        out = self.centre.secondary_index(df, parent_res)
        col = f"{self.DGGS}_{parent_res:02}"
        for cell, parent in zip(out.index, out[col], strict=True):
            self.assertEqual(self.centre.get_resolution(int(parent)), parent_res)
            self.assertTrue(
                self.dggrs.isZoneDescendantOf(
                    int(cell), int(parent), self.res - parent_res
                )
            )

    def test_children_area_and_max_res_follow_the_grid(self):
        cell = self.centre._cell_at(174.7, -41.3, self.res - 2)
        ratio = self.dggrs.getRefinementRatio()
        self.assertEqual(len(self.centre.children_at_res(cell, self.res)), ratio**2)
        self.assertEqual(self.centre.children_at_res(cell, self.res - 2), [cell])
        self.assertAlmostEqual(
            self.centre.cell_area_m2(self.res),
            float(self.dggrs.getZoneArea(self.centre._cell_at(174.7, -41.3, self.res))),
            delta=self.centre.cell_area_m2(self.res) * 1e-6,
        )
        self.assertEqual(self.centre.max_res, self.dggrs.getMaxDGGRSZoneLevel())

    def test_compaction_round_trips_and_respects_the_floor(self):
        parent_res = self.res - 2
        ancestor = self.centre._cell_at(174.7, -41.3, parent_res - 1)
        full = self.centre.children_at_res(ancestor, self.res)
        lone = self.centre._cell_at(174.0, -41.0, self.res)
        cells = [*full, lone]
        df = pd.DataFrame(
            {
                "id": [1] * len(cells),
                "attr": range(len(cells)),
                "c": pd.array(cells, dtype="uint64"),
            }
        ).set_index("c")
        out = self.centre.compaction(
            df, self.res, ["id", "attr"], "c", "id", parent_res
        )
        self.assertTrue(
            all(self.centre.get_resolution(int(c)) >= parent_res for c in out.index)
        )
        expanded = {
            int(x)
            for c in out.index
            for x in self.centre.children_at_res(int(c), self.res)
        }
        self.assertEqual(expanded, set(cells))
        self.assertLess(len(out), len(cells))

    def test_text_ids_round_trip_and_metadata_names_the_grid(self):
        cell = self.centre._cell_at(174.7, -41.3, self.res)
        (text,) = self.centre.cells_to_string([cell])
        self.assertEqual(self.centre._zone(text), cell)
        self.assertEqual(self.centre.get_resolution(text), self.res)
        meta = self.centre.grid_metadata()
        self.assertEqual(meta["grid"], self.centre.DGGRS_CLASS)
        self.assertIsNotNone(self.centre.library_versions()["dggal"])

    def test_resolution_floor_helper(self):
        ancestor = self.centre._cell_at(174.7, -41.3, self.res - 3)
        result = VectorIndexer._enforce_resolution_floor(
            {ancestor},
            self.res - 2,
            self.centre.get_resolution,
            self.centre.children_at_res,
        )
        self.assertEqual(
            set(result), set(self.centre.children_at_res(ancestor, self.res - 2))
        )


class TestISEA9RGrid(DGGALGridScenarios, TestCase):
    DGGS = "isea9r"


class TestHEALPixGrid(DGGALGridScenarios, TestCase):
    DGGS = "healpix"


class TestISEA4RGrid(DGGALGridScenarios, TestCase):
    DGGS = "isea4r"
