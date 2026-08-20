from shapely import has_z, wkt
from shapely.geometry import LinearRing, Polygon

from vector2dggs.katana import katana

from .base import TestRunthrough

polygon_a = wkt.loads(
    "POLYGON ((-62.490234 37.09024, -87.363281 30.524413, -86.484375 21.616579, "
    "-81.5625 15.029686, -64.248047 16.972741, -64.599609 25.562265, "
    "-70.576172 25.641526, -59.941406 31.052934, -52.470703 35.317366, "
    "-62.490234 37.09024))"
)
polygon_b = wkt.loads(
    "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))"
)
polygon_c = wkt.loads(
    "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)),((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20)))"
)
polygon_d = wkt.loads(
    "POLYGON ((-42.978516 31.952162, -65.478516 29.382175, -47.460937 26.509905, -72.509766 19.311143, -48.779297 20.632784, -47.109375 11.436955, -44.033203 21.207459, -34.980469 16.467695, -40.869141 23.483401, -27.333984 26.037042, -35.068359 28.304381, -43.242188 27.605671, -42.978516 31.952162))"
)


class TestKatana(TestRunthrough):
    """
    Sends test data through katana cutting.
    """

    def test_katana(self):
        area_threshold = 0.05
        for geom in [polygon_a, polygon_b, polygon_c, polygon_d]:
            katana(geom, area_threshold)

    def test_katana_respects_threshold(self):
        square = Polygon([(0, 0), (100, 0), (100, 100), (0, 100)])
        threshold = 10.0
        parts = katana(square, threshold)
        for part in parts:
            minx, miny, maxx, maxy = part.bounds
            self.assertLessEqual((maxx - minx) * (maxy - miny), threshold)
        # No area may be lost or duplicated by the splitting
        self.assertAlmostEqual(sum(p.area for p in parts), square.area, places=6)

    def test_linearring_input_becomes_polygon(self):
        ring = LinearRing([(0, 0), (10, 0), (10, 10), (0, 10)])
        pieces = katana(ring, threshold=1000.0)
        self.assertEqual([p.geom_type for p in pieces], ["Polygon"])

    def test_3d_input_is_flattened(self):
        poly = Polygon([(0, 0, 5), (10, 0, 5), (10, 10, 5), (0, 10, 5)])
        pieces = katana(poly, threshold=30.0)
        self.assertGreater(len(pieces), 1)
        self.assertFalse(any(has_z(p) for p in pieces))

    def test_invalid_bowtie_is_made_valid(self):
        bowtie = Polygon([(0, 0), (10, 10), (10, 0), (0, 10)])  # self-intersecting
        pieces = katana(bowtie, threshold=30.0)
        self.assertTrue(all(p.is_valid for p in pieces))
        # a bowtie's true area is the two triangles
        self.assertAlmostEqual(sum(p.area for p in pieces), 50.0, places=6)
