from unittest import TestCase

import click
import geopandas as gpd
from shapely.geometry import LineString, Point, Polygon, box

import vector2dggs.constants as const
from vector2dggs import common
from vector2dggs.a5 import a5
from vector2dggs.indexerfactory import indexer_instance

from .base import skip_unless_backend

# Fine enough that the test polygon spans several cells either way, coarse
# enough that every mode stays cheap (A5 subdivides fastest, hence its
# lower level here than the equivalent-scale ones elsewhere).
RES = {"h3": 8, "s2": 13, "a5": 15, "rhp": 8, "geohash": 6}

# Several cells across at every RES above, with a boundary that cuts through
# cells rather than following them, so the three modes must disagree.
BIG = Polygon([(174.70, -41.30), (174.75, -41.30), (174.75, -41.25), (174.70, -41.25)])
ANCHOR = Point(174.72, -41.27)  # inside BIG; picks the cell the tiny box sits on

MODES = tuple(const.ContainmentMode)


class ContainmentModeScenarios:
    """
    The -m/--mode contract, asserted per backend against its own cells.

    The three modes are nested by construction - a cell wholly inside a
    feature necessarily has its centre inside it, and a cell with its centre
    inside necessarily meets it - so within <= centre <= intersects holds
    for any backend whose cell area and cell centre are mutually
    consistent,
    whatever shape it actually models a cell as. That, and the coverage
    guarantee that gives intersects mode its purpose, is what these tests
    pin.
    """

    DGGS: str

    @classmethod
    def setUpClass(cls):
        skip_unless_backend(cls.DGGS)
        super().setUpClass()

    # -- helpers -----------------------------------------------------------

    @property
    def res(self) -> int:
        return RES[self.DGGS]

    def _supported(self, mode: const.ContainmentMode) -> bool:
        return mode in indexer_instance(self.DGGS).SUPPORTED_MODES

    def _cells(self, geom, mode: const.ContainmentMode) -> set:
        indexer = indexer_instance(self.DGGS, mode.value)
        df = gpd.GeoDataFrame({"fid": [1], "geometry": [geom]}, crs=4326)
        return set(indexer.polyfill(df, self.res).index)

    def _tiny_polygon(self):
        """
        A polygon far smaller than a cell, sitting on a cell vertex.

        Deliberately built from the backend's own geometry rather than as a
        fixed literal: at a vertex, where cells meet, it is as far from any
        cell centre as a box that size can be, so "no cell centre falls
        inside it" holds for any backend's cells, not just the ones whose
        numbers happened to work out when the test was written.
        """
        indexer = indexer_instance(self.DGGS)
        point = gpd.GeoDataFrame({"fid": [1], "geometry": [ANCHOR]}, crs=4326)
        cell = next(iter(indexer.polyfill(point, self.res).index))
        cell_polygon = indexer.cell_to_polygon(cell)
        vx, vy = cell_polygon.exterior.coords[0]
        minx, _, maxx, _ = cell_polygon.bounds
        half = (maxx - minx) / 100
        return box(vx - half, vy - half, vx + half, vy + half)

    # -- the mode contract -------------------------------------------------

    def test_modes_are_nested(self):
        centre = self._cells(BIG, const.ContainmentMode.CENTRE)
        intersects = self._cells(BIG, const.ContainmentMode.INTERSECTS)
        self.assertLess(centre, intersects, "intersects must strictly contain centre")
        if self._supported(const.ContainmentMode.WITHIN):
            within = self._cells(BIG, const.ContainmentMode.WITHIN)
            self.assertLess(within, centre, "centre must strictly contain within")
            self.assertTrue(within, "within mode found no cells to compare")

    def test_default_mode_is_centre(self):
        indexer = indexer_instance(self.DGGS)
        self.assertEqual(indexer.mode, const.ContainmentMode.CENTRE)
        df = gpd.GeoDataFrame({"fid": [1], "geometry": [BIG]}, crs=4326)
        self.assertEqual(
            set(indexer.polyfill(df, self.res).index),
            self._cells(BIG, const.ContainmentMode.CENTRE),
        )

    def test_feature_smaller_than_a_cell(self):
        """
        The reason intersects mode exists: a feature too small to capture
        a cell centre is silently dropped in centre mode (and in within
        mode, which no sub-cell feature can ever satisfy), but always
        indexed in intersects mode.
        """
        tiny = self._tiny_polygon()
        self.assertEqual(self._cells(tiny, const.ContainmentMode.CENTRE), set())
        self.assertTrue(self._cells(tiny, const.ContainmentMode.INTERSECTS))
        if self._supported(const.ContainmentMode.WITHIN):
            self.assertEqual(self._cells(tiny, const.ContainmentMode.WITHIN), set())

    def test_linestrings_and_points_are_mode_invariant(self):
        """
        Only polygon filling varies by mode: a traced line already visits
        every cell it touches, and a point has exactly one cell.
        """
        df = gpd.GeoDataFrame(
            {
                "fid": [1, 2],
                "geometry": [
                    LineString([(174.70, -41.30), (174.75, -41.25)]),
                    ANCHOR,
                ],
            },
            crs=4326,
        )
        expected = None
        for mode in MODES:
            if not self._supported(mode):
                continue
            with self.subTest(mode=mode.value):
                indexer = indexer_instance(self.DGGS, mode.value)
                cells = set(indexer.polyfill(df, self.res).index)
                self.assertTrue(cells)
                if expected is None:
                    expected = cells
                self.assertEqual(cells, expected)

    def test_unsupported_modes_are_rejected(self):
        """
        A mode the backend's library cannot express must fail with a clear
        message, never fall back to another mode's answer.
        """
        indexer = indexer_instance(self.DGGS)
        for mode in MODES:
            with self.subTest(mode=mode.value):
                if self._supported(mode):
                    common.check_mode(mode.value, indexer)  # does not raise
                else:
                    with self.assertRaisesRegex(
                        common.ContainmentModeError, mode.value
                    ):
                        common.check_mode(mode.value, indexer)


class TestH3ContainmentMode(ContainmentModeScenarios, TestCase):
    DGGS = "h3"


class TestS2ContainmentMode(ContainmentModeScenarios, TestCase):
    DGGS = "s2"


class TestA5ContainmentMode(ContainmentModeScenarios, TestCase):
    DGGS = "a5"


class TestRHPContainmentMode(ContainmentModeScenarios, TestCase):
    DGGS = "rhp"


class TestGeohashContainmentMode(ContainmentModeScenarios, TestCase):
    DGGS = "geohash"


class TestA5WithinUnsupported(TestCase):
    """
    A5 is the one backend with no wholly-within test (pya5's containment
    option offers 'center' and 'overlapping' only), so it is the case that
    pins the unsupported-mode path end to end.
    """

    def setUp(self):
        skip_unless_backend("a5")

    def test_indexer_declares_no_within_mode(self):
        self.assertNotIn(
            const.ContainmentMode.WITHIN,
            indexer_instance("a5").SUPPORTED_MODES,
        )

    def test_polyfill_rejects_within(self):
        """
        The library path, not just the CLI: pya5 accepts an unrecognised
        containment value and quietly applies centre containment, so an
        unsupported mode has to be refused here rather than left to
        surface as a KeyError, or worse as a plausible wrong answer.
        """
        indexer = indexer_instance("a5", "within")
        frame = gpd.GeoDataFrame({"fid": [1], "geometry": [BIG]}, crs=4326)
        with self.assertRaises(common.ContainmentModeError) as caught:
            indexer.polyfill(frame, RES["a5"])
        self.assertIn("within", str(caught.exception))
        self.assertIn("centre, intersects", str(caught.exception))

    def test_cli_rejects_within(self):
        with self.assertRaises(click.UsageError) as caught:
            a5(
                ["in.gpkg", "out.pq", "-r", "10", "-m", "within"],
                standalone_mode=False,
            )
        self.assertIn("within", str(caught.exception))

    def test_cli_help_omits_within(self):
        """The choice list a user is shown must match what A5 can do."""
        help_text = a5.get_help(click.Context(a5, info_name="a5"))
        self.assertIn("[centre|intersects]", help_text.replace("\n", " "))
