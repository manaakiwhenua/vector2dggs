from unittest import TestCase

import geopandas as gpd
import h3
from h3.api import basic_int
from shapely.geometry import LineString, Polygon
from shapely.prepared import prep

from vector2dggs.indexerfactory import indexer_instance

from .base import skip_unless_backend

RES = {"h3": 9, "s2": 14, "rhp": 9, "geohash": 6}

# A closed ring and an open line, both with diagonal runs: a path-based
# tracer survives axis-aligned geometry and fails on diagonals, where it
# routes around a clipped corner rather than through it.
RING = LineString(
    [
        (174.70, -41.30),
        (174.76, -41.24),
        (174.80, -41.28),
        (174.74, -41.32),
        (174.70, -41.30),
    ]
)
OPEN = LineString([(174.70, -41.30), (174.76, -41.24), (174.80, -41.28)])


def _traced(dggs: str, line: LineString, res: int) -> set:
    indexer = indexer_instance(dggs)
    df = gpd.GeoDataFrame({"fid": [1], "geometry": [line]}, crs=4326)
    return set(indexer.polyfill(df, res).index)


class TestLineCover(TestCase):
    """
    Line tracing must return a *cover* - every cell whose area the line
    meets - and not a path, a route of adjacent cells joining the line's
    ends. A path is free to step through a cell the line never enters and
    to skip one it merely clips, which is correct for navigation and wrong
    for indexing.

    Checked against an oracle built independently of the tracer: the cells
    of a dilated candidate set whose polygon actually meets the line.
    """

    def _oracle(self, indexer, line: LineString, res: int, got: set) -> set:
        # dilate the tracer's own answer by a ring of neighbours, then test
        # geometrically: any cell the tracer wrongly omitted is adjacent to
        # one it found, so this cannot miss an omission
        candidates = set(got)
        for cell in got:
            candidates.update(indexer._neighbours(cell))
        prepared = prep(line)
        return {
            c for c in candidates if prepared.intersects(indexer.cell_to_polygon(c))
        }

    def _assert_cover(self, dggs: str, line: LineString):
        skip_unless_backend(dggs)
        indexer = indexer_instance(dggs)
        try:
            indexer._neighbours(next(iter(_traced(dggs, line, RES[dggs]))))
        except NotImplementedError:
            self.skipTest(f"{dggs} traces lines with its library's own cover")
        got = _traced(dggs, line, RES[dggs])
        oracle = self._oracle(indexer, line, RES[dggs], got)
        self.assertTrue(oracle)
        self.assertEqual(
            got & oracle,
            oracle,
            f"{dggs}: {len(oracle - got)} cells the line meets were not traced",
        )
        self.assertFalse(
            got - oracle, f"{dggs}: {len(got - oracle)} traced cells the line misses"
        )

    def test_h3_ring(self):
        self._assert_cover("h3", RING)

    def test_h3_open_line(self):
        self._assert_cover("h3", OPEN)

    def test_geohash_ring(self):
        self._assert_cover("geohash", RING)

    def test_geohash_open_line(self):
        self._assert_cover("geohash", OPEN)


class TestH3CoverAgainstH3(TestCase):
    """
    The H3 cover, checked against H3's own containment rather than against
    a second geometric implementation of ours that could be wrong in the
    same way.

    A polygon traced out and back encloses no area, so asking H3 for the
    cells overlapping it asks exactly which cells its boundary passes
    through - decided by H3's own geometry. It is unusable in the pipeline,
    costing bbox area rather than line length (measured at res 13: 5.4s
    against the flood fill's 1.2s for the same 6,762 cells, and widening),
    but that makes it an independent oracle, not a replacement.
    """

    def setUp(self):
        skip_unless_backend("h3")

    @staticmethod
    def _h3_boundary_cells(line: LineString, res: int) -> set:
        latlng = [(lat, lon) for lon, lat in line.coords]
        degenerate = h3.LatLngPoly(latlng + latlng[::-1])
        return set(basic_int.h3shape_to_cells_experimental(degenerate, res, "overlap"))

    def test_cover_matches_h3s_own_geometry(self):
        for label, line in (("ring", RING), ("open", OPEN)):
            for res in (8, 9, 10):
                with self.subTest(line=label, res=res):
                    self.assertEqual(
                        _traced("h3", line, res),
                        self._h3_boundary_cells(line, res),
                    )

    def test_cover_is_the_boundary_of_the_filled_polygon(self):
        """
        Closing the loop on why any of this matters: for a closed ring, the
        cover is exactly the cells of the polygon that are not wholly
        inside it - which is what makes within = intersects \\ cover(boundary)
        hold, and what a path-based tracer cannot deliver.
        """
        res = 9
        polygon = Polygon(RING.coords)
        frame = gpd.GeoDataFrame({"fid": [1], "geometry": [polygon]}, crs=4326)
        intersects = set(
            indexer_instance("h3", "intersects").polyfill(frame, res).index
        )
        within = set(indexer_instance("h3", "within").polyfill(frame, res).index)
        self.assertEqual(_traced("h3", RING, res), intersects - within)


class TestWithinIsIntersectsMinusCover(TestCase):
    """
    The identity the pipeline composes WITHIN from, checked per backend
    against that backend's own native wholly-within answer.

    It has to hold for every backend independently, because each decides
    all three of intersects, within and the cover against its own model of
    a cell's extent; an identity that held only in the abstract would put
    cells either side of a feature's boundary into the wrong set.
    """

    SHELL = [(174.70, -41.30), (174.80, -41.30), (174.80, -41.20), (174.70, -41.20)]
    HOLE = [(174.73, -41.27), (174.77, -41.27), (174.77, -41.23), (174.73, -41.23)]

    def _assert_identity(self, dggs: str, polygon: Polygon):
        skip_unless_backend(dggs)
        res = RES[dggs]
        frame = gpd.GeoDataFrame({"fid": [1], "geometry": [polygon]}, crs=4326)
        intersects = set(
            indexer_instance(dggs, "intersects").polyfill(frame, res).index
        )
        within = set(indexer_instance(dggs, "within").polyfill(frame, res).index)
        self.assertTrue(within, "no cells lie within the test polygon")
        # every ring, holes included: a cell straddling a hole's edge is no
        # more within the feature than one straddling its outer edge
        rings = [LineString(polygon.exterior.coords)] + [
            LineString(i.coords) for i in polygon.interiors
        ]
        lines = gpd.GeoDataFrame({"fid": [1] * len(rings), "geometry": rings}, crs=4326)
        cover = set(indexer_instance(dggs).polyfill(lines, res).index)
        self.assertEqual(intersects - cover, within)

    def test_h3(self):
        self._assert_identity("h3", Polygon(self.SHELL))

    def test_s2(self):
        self._assert_identity("s2", Polygon(self.SHELL))

    def test_rhp(self):
        self._assert_identity("rhp", Polygon(self.SHELL))

    def test_geohash(self):
        self._assert_identity("geohash", Polygon(self.SHELL))

    def test_h3_with_a_hole(self):
        self._assert_identity("h3", Polygon(self.SHELL, [self.HOLE]))

    def test_rhp_with_a_hole(self):
        self._assert_identity("rhp", Polygon(self.SHELL, [self.HOLE]))
