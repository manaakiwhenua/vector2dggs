import json

import pandas as pd
import pyarrow.parquet as pq

import vector2dggs.constants as const
from vector2dggs.a5 import a5
from vector2dggs.geohash import geohash
from vector2dggs.h3 import h3
from vector2dggs.indexerfactory import indexer_instance
from vector2dggs.rHP import rhp
from vector2dggs.s2 import s2

from ..data.datapaths import (
    TEST_FILE_PATH,
    TEST_LAYER_NAME,
    TEST_LINESTRING_FILE_PATH,
    TEST_LINESTRING_LAYER_NAME,
    TEST_POINT_FILE_PATH,
    TEST_POINT_LAYER_NAME,
)
from .base import TestRunthrough, skip_unless_backend

POLYGON = (TEST_FILE_PATH, TEST_LAYER_NAME, "LCDB_UID", "Name_2018")
LINESTRING = (
    TEST_LINESTRING_FILE_PATH,
    TEST_LINESTRING_LAYER_NAME,
    "t50_fid",
    "elevation",
)
POINT = (TEST_POINT_FILE_PATH, TEST_POINT_LAYER_NAME, "t50_fid", "elevation")


class RunthroughScenarios:
    """
    The 20 CLI smoke scenarios, shared by every backend. Subclasses set the
    command and per-backend resolutions; every scenario asserts on the output.
    """

    DGGS: str
    COMMAND: staticmethod
    POLYGON_RES: str
    LINE_RES: str  # also used for point inputs

    @classmethod
    def setUpClass(cls):
        skip_unless_backend(cls.DGGS)
        super().setUpClass()

    # -- helpers -----------------------------------------------------------

    def _run(self, dataset, res, *extra):
        path, layer, _, _ = dataset
        self.COMMAND(
            [
                path,
                str(self.output_path),
                "--layer",
                layer,
                "-r",
                res,
                "-t",
                "1",
                *extra,
            ],
            standalone_mode=False,
        )

    def _assert_output(self, res, id_col="fid", attr_col=None, geo=None, compact=False):
        files = sorted(self.output_path.rglob("*.parquet"))
        self.assertTrue(files, "no parquet output written")

        df = pd.read_parquet(self.output_path)
        self.assertGreater(len(df), 0, "output has no rows")

        dggs_col = f"{self.DGGS}_{int(res):02}"
        parent_res = const.DEFAULT_DGGS_PARENT_RES[self.DGGS](int(res))
        parent_col = f"{self.DGGS}_{parent_res:02}"
        self.assertEqual(df.index.name, dggs_col)
        self.assertIn(id_col, df.columns)
        if attr_col is not None:
            self.assertIn(attr_col, df.columns)

        indexer = indexer_instance(self.DGGS)
        resolutions = {indexer.get_resolution(c) for c in df.index}
        if compact:
            self.assertTrue(all(parent_res <= r <= int(res) for r in resolutions))
        else:
            self.assertEqual(resolutions, {int(res)})

        # hive partition dirs carry the parent cell
        partition_dirs = [d for d in self.output_path.iterdir() if d.is_dir()]
        self.assertTrue(
            all(d.name.startswith(f"{parent_col}=") for d in partition_dirs)
        )

        if geo is not None:
            table = pq.read_table(files[0])
            meta = table.schema.metadata or {}
            self.assertIn(b"geo", meta, "GeoParquet metadata missing")
            geo_meta = json.loads(meta[b"geo"])
            geom_types = geo_meta["columns"]["geometry"].get("geometry_types", [])
            expected = "Point" if geo == "point" else "Polygon"
            self.assertTrue(all(t == expected for t in geom_types), geom_types)

    # -- polygon input -----------------------------------------------------

    def test_run(self):
        self._run(POLYGON, self.POLYGON_RES)
        self._assert_output(self.POLYGON_RES)

    def test_run_overwrite(self):
        self._run(POLYGON, self.POLYGON_RES)
        self._run(POLYGON, self.POLYGON_RES, "-o")
        self._assert_output(self.POLYGON_RES)

    def test_cut_crs(self):
        self._run(POLYGON, self.POLYGON_RES, "-crs", "3793", "-c", "4000")
        self._assert_output(self.POLYGON_RES)

    def test_cut_crs_reproject(self):
        self._run(POLYGON, self.POLYGON_RES, "-crs", "4326", "-c", "0.005")
        self._assert_output(self.POLYGON_RES)

    def test_no_bisection(self):
        self._run(POLYGON, self.POLYGON_RES, "-c", "0")
        self._assert_output(self.POLYGON_RES)

    def test_bisection_invariance(self):
        """Bisection must be invisible: identical (cell, feature) sets with
        and without cutting."""
        self._run(POLYGON, self.POLYGON_RES, "-c", "0")
        uncut = pd.read_parquet(self.output_path)
        uncut_cells = set(zip(uncut.index, uncut["fid"], strict=True))
        self._run(POLYGON, self.POLYGON_RES, "-o", "-c", "300000")
        cut = pd.read_parquet(self.output_path)
        cut_cells = set(zip(cut.index, cut["fid"], strict=True))
        self.assertEqual(uncut_cells, cut_cells)

    def test_compaction(self):
        self._run(POLYGON, self.POLYGON_RES, "-co", "-id", POLYGON[2])
        self._assert_output(self.POLYGON_RES, id_col=POLYGON[2], compact=True)

    def test_geo_point(self):
        self._run(POLYGON, self.POLYGON_RES, "--geo", "point")
        self._assert_output(self.POLYGON_RES, geo="point")

    def test_geo_point_compact(self):
        self._run(POLYGON, self.POLYGON_RES, "--geo", "point", "-co", "-id", POLYGON[2])
        self._assert_output(
            self.POLYGON_RES, id_col=POLYGON[2], geo="point", compact=True
        )

    def test_geo_polygon(self):
        self._run(POLYGON, self.POLYGON_RES, "--geo", "polygon")
        self._assert_output(self.POLYGON_RES, geo="polygon")

    def test_geo_polygon_compact(self):
        self._run(
            POLYGON, self.POLYGON_RES, "--geo", "polygon", "-co", "-id", POLYGON[2]
        )
        self._assert_output(
            self.POLYGON_RES, id_col=POLYGON[2], geo="polygon", compact=True
        )

    # -- linestring input --------------------------------------------------

    def test_linestring_run(self):
        self._run(LINESTRING, self.LINE_RES, "-c", "0")
        self._assert_output(self.LINE_RES)

    def test_linestring_keep_attrs(self):
        self._run(LINESTRING, self.LINE_RES, "-c", "0", "-k")
        self._assert_output(self.LINE_RES, attr_col=LINESTRING[3])

    def test_linestring_compaction(self):
        self._run(LINESTRING, self.LINE_RES, "-c", "0", "-co", "-id", LINESTRING[2])
        self._assert_output(self.LINE_RES, id_col=LINESTRING[2], compact=True)

    def test_linestring_geo_point(self):
        self._run(LINESTRING, self.LINE_RES, "-c", "0", "--geo", "point")
        self._assert_output(self.LINE_RES, geo="point")

    def test_linestring_geo_polygon(self):
        self._run(LINESTRING, self.LINE_RES, "-c", "0", "--geo", "polygon")
        self._assert_output(self.LINE_RES, geo="polygon")

    # -- point input -------------------------------------------------------

    def test_point_run(self):
        self._run(POINT, self.LINE_RES, "-c", "0")
        self._assert_output(self.LINE_RES)

    def test_point_keep_attrs(self):
        self._run(POINT, self.LINE_RES, "-c", "0", "-k")
        self._assert_output(self.LINE_RES, attr_col=POINT[3])

    def test_point_compaction(self):
        self._run(POINT, self.LINE_RES, "-c", "0", "-co", "-id", POINT[2])
        self._assert_output(self.LINE_RES, id_col=POINT[2], compact=True)

    def test_point_geo_point(self):
        self._run(POINT, self.LINE_RES, "-c", "0", "--geo", "point")
        self._assert_output(self.LINE_RES, geo="point")

    def test_point_geo_polygon(self):
        self._run(POINT, self.LINE_RES, "-c", "0", "--geo", "polygon")
        self._assert_output(self.LINE_RES, geo="polygon")


class TestH3(RunthroughScenarios, TestRunthrough):
    DGGS = "h3"
    COMMAND = staticmethod(h3)
    POLYGON_RES = "8"
    LINE_RES = "10"


class TestS2(RunthroughScenarios, TestRunthrough):
    DGGS = "s2"
    COMMAND = staticmethod(s2)
    POLYGON_RES = "13"
    LINE_RES = "13"


class TestA5(RunthroughScenarios, TestRunthrough):
    DGGS = "a5"
    COMMAND = staticmethod(a5)
    POLYGON_RES = "17"
    LINE_RES = "17"


class TestRHP(RunthroughScenarios, TestRunthrough):
    DGGS = "rhp"
    COMMAND = staticmethod(rhp)
    POLYGON_RES = "8"
    LINE_RES = "8"


class TestGeohash(RunthroughScenarios, TestRunthrough):
    DGGS = "geohash"
    COMMAND = staticmethod(geohash)
    POLYGON_RES = "6"
    LINE_RES = "6"
