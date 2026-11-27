import warnings
from unittest import TestCase

import click
import geopandas as gpd
import pandas as pd
from rhealpixdggs.dggs import RHEALPixDGGS
from shapely.geometry import Point

import vector2dggs.constants as const
from vector2dggs import common
from vector2dggs.h3 import h3
from vector2dggs.indexerfactory import indexer_instance
from vector2dggs.rHP import rhp

from .base import TestRunthrough, skip_unless_backend

OGC_URI = "https://www.opengis.net/def/dggrs/OGC/1.0/rHEALPix"
# The same level-3 cell over New Zealand, named in each grid: the grids'
# equatorial cells coincide from level 2 up, 50 degrees being a whole number
# of level-2 cells.
NZ = Point(174.0, -41.0)
NZ_OGC, NZ_STD = "R767", "R887"
POLE = Point(0.0, 85.0)


class TestRhpGridOption(TestRunthrough):
    @classmethod
    def setUpClass(cls):
        skip_unless_backend("rhp")
        super().setUpClass()

    def setUp(self):
        super().setUp()
        self.src = self.output_path.parent / "pt.gpkg"
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            gpd.GeoDataFrame({"fid": [1], "geometry": [NZ]}, crs=4326).to_file(
                self.src, layer="pt"
            )

    def _cells(self, out, extra=()):
        rhp(
            [str(self.src), str(out), "-r", "3", "-t", "1", *extra],
            standalone_mode=False,
        )
        return set(pd.read_parquet(out).index)

    def test_default_is_the_ogc_grid(self):
        self.assertEqual(self._cells(self.output_path / "default"), {NZ_OGC})

    def test_registered_uri_either_scheme(self):
        for uri in (OGC_URI, OGC_URI.replace("https://", "http://")):
            with self.subTest(uri=uri):
                self.assertEqual(
                    self._cells(self.output_path / uri[:5], ("--grid", uri)),
                    {NZ_OGC},
                )

    def test_library_call_takes_grid(self):
        common.index(
            "rhp",
            self.src,
            self.output_path / "lib",
            3,
            None,
            False,
            1,
            layer="pt",
            grid="003",
        )
        self.assertEqual(set(pd.read_parquet(self.output_path / "lib").index), {NZ_STD})

    def test_unregistered_uri_is_rejected_naming_the_registered_ones(self):
        with self.assertRaisesRegex(ValueError, OGC_URI):
            self._cells(self.output_path / "bad", ("--grid", "https://example.org/x"))

    def test_unrecognised_form_is_rejected_at_the_cli(self):
        # library preset names are deliberately not a form
        with self.assertRaisesRegex(click.BadParameter, "DGGRS URI"):
            self._cells(self.output_path / "bad", ("--grid", "WGS84_003"))

    def test_spec_form(self):
        self.assertEqual(
            self._cells(self.output_path / "s1", ("--grid", "003@50")), {NZ_OGC}
        )
        self.assertEqual(
            self._cells(self.output_path / "s2", ("--grid", "003")), {NZ_STD}
        )
        # north square 1, south square 1, meridian 0: a third naming of NZ
        expected = str(
            RHEALPixDGGS(north_square=1, south_square=1).cell_from_point(
                3, (NZ.x, NZ.y), plane=False
            )
        )
        self.assertEqual(
            self._cells(self.output_path / "s3", ("--grid", "113")), {expected}
        )

    def test_spec_with_other_n_side_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "N_side 2"):
            self._cells(self.output_path / "bad", ("--grid", "002"))

    def test_grid_option_absent_on_single_grid_backends(self):
        with self.assertRaises(click.NoSuchOption):
            h3(
                [str(self.src), str(self.output_path / "h3"), "-r", "3", "--grid", "x"],
                standalone_mode=False,
            )
        with self.assertRaises(ValueError):
            indexer_instance("h3", grid="003")


class TestRhpGridGeometry(TestCase):
    """What differs between the grids, pinned so the README stays true."""

    def setUp(self):
        skip_unless_backend("rhp")
        self.ogc = indexer_instance("rhp", grid=const.RHP_DEFAULT_GRID)
        self.std = indexer_instance("rhp", grid="003")

    def _cell(self, indexer, point, resolution):
        df = gpd.GeoDataFrame({"fid": [1], "geometry": [point]}, crs=4326)
        return indexer._polyfill_points(df, resolution).index[0]

    def test_default_resolves_to_the_ogc_grid(self):
        self.assertEqual(indexer_instance("rhp").grid.codespace, OGC_URI)
        self.assertIsNone(self.std.grid.codespace)

    def test_spec_matching_a_registered_grid_keeps_its_codespace(self):
        self.assertIs(indexer_instance("rhp", grid="003@50").grid, self.ogc.grid)
        custom = indexer_instance("rhp", grid="123@50").grid
        self.assertIsNone(custom.codespace)
        self.assertEqual((custom.north_square, custom.south_square), (1, 2))
        self.assertEqual(float(custom.ellipsoid.lon_0), 50.0)

    def test_equatorial_cells_coincide_under_different_names(self):
        a, b = self._cell(self.ogc, NZ, 3), self._cell(self.std, NZ, 3)
        self.assertEqual((a, b), (NZ_OGC, NZ_STD))
        self.assertTrue(
            self.ogc.cell_to_polygon(a).equals_exact(self.std.cell_to_polygon(b), 1e-9)
        )

    def test_polar_cells_differ(self):
        a, b = self._cell(self.ogc, POLE, 3), self._cell(self.std, POLE, 3)
        self.assertFalse(
            self.ogc.cell_to_polygon(a).equals_exact(self.std.cell_to_polygon(b), 1e-6)
        )

    def test_level_one_cells_do_not_coincide(self):
        a, b = self._cell(self.ogc, NZ, 1), self._cell(self.std, NZ, 1)
        self.assertFalse(
            self.ogc.cell_to_polygon(a).equals_exact(self.std.cell_to_polygon(b), 1e-6)
        )
