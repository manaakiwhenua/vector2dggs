import tempfile
import warnings
from pathlib import Path
from unittest import TestCase

import click
import geopandas as gpd
from pyogrio.errors import DataLayerError
from shapely.geometry import Polygon

from vector2dggs import common
from vector2dggs.h3 import h3
from vector2dggs.indexerfactory import indexer_instance

from ..data.datapaths import TEST_FILE_PATH, TEST_LAYER_NAME
from .base import TestRunthrough


class TestErrors(TestRunthrough):
    """Error-path unit tests that raise before writing any output."""

    def test_crsless_input_raises_clear_error(self):
        naive = gpd.GeoDataFrame(
            {"geometry": [Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])]}
        )
        with self.assertRaisesRegex(ValueError, "CRS"):
            common.bisection_preparation(naive, "h3", 5, None, None)

    def test_crsless_file_rejected_before_any_processing(self):
        naive = gpd.GeoDataFrame(
            {"geometry": [Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])]}
        )
        with tempfile.TemporaryDirectory() as d:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                naive.to_file(f"{d}/naive.gpkg", layer="naive")
            with self.assertRaisesRegex(ValueError, "CRS"):
                common.index(
                    "h3",
                    f"{d}/naive.gpkg",
                    f"{d}/out.pq",
                    9,
                    5,
                    False,
                    None,
                    1,
                    layer="naive",
                )

    def test_invalid_compression_raises(self):
        with self.assertRaises(click.BadParameter):
            h3(
                [
                    TEST_FILE_PATH,
                    str(self.output_path),
                    "--layer",
                    TEST_LAYER_NAME,
                    "-r",
                    "8",
                    "-cp",
                    "bogus",
                ],
                standalone_mode=False,
            )

    def test_zero_threads_raises(self):
        with self.assertRaises(click.BadParameter):
            h3(
                [
                    TEST_FILE_PATH,
                    str(self.output_path),
                    "--layer",
                    TEST_LAYER_NAME,
                    "-r",
                    "8",
                    "-t",
                    "0",
                ],
                standalone_mode=False,
            )

    def test_parent_res_not_less_than_resolution_raises(self):
        with self.assertRaises(common.ParentResolutionException):
            h3(
                [
                    TEST_FILE_PATH,
                    str(self.output_path),
                    "--layer",
                    TEST_LAYER_NAME,
                    "-r",
                    "8",
                    "-pr",
                    "8",
                ],
                standalone_mode=False,
            )

    def test_compact_without_id_field_raises(self):
        with self.assertRaises(common.IdFieldError):
            h3(
                [
                    TEST_FILE_PATH,
                    str(self.output_path),
                    "--layer",
                    TEST_LAYER_NAME,
                    "-r",
                    "8",
                    "-co",
                ],
                standalone_mode=False,
            )

    def test_unknown_dggs_raises(self):
        with self.assertRaises(ValueError):
            indexer_instance("not_a_real_dggs")

    def test_unknown_keep_attribute_raises(self):
        with self.assertRaises(common.UnknownAttributeError):
            h3(
                [
                    TEST_FILE_PATH,
                    str(self.output_path),
                    "--layer",
                    TEST_LAYER_NAME,
                    "-r",
                    "8",
                    "-ka",
                    "not_a_real_column",
                ],
                standalone_mode=False,
            )


class TestIndexCompactionDefaults(TestCase):
    """Library API: index() must validate compaction requirements itself."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        df = gpd.GeoDataFrame(
            {
                "geometry": [
                    Polygon(
                        [(174.7, -41.3), (174.9, -41.3), (174.9, -41.2), (174.7, -41.2)]
                    )
                ]
            },
            crs=4326,
        )
        self.src = f"{self._tmp.name}/in.gpkg"
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df.to_file(self.src, layer="x")

    def tearDown(self):
        self._tmp.cleanup()

    def _index(self, **kwargs):
        return common.index(
            "h3",
            self.src,
            f"{self._tmp.name}/out.pq",
            7,
            None,
            False,
            0.0,
            1,
            layer="x",
            **kwargs,
        )

    def test_compact_without_id_field_raises_idfielderror(self):
        with self.assertRaises(common.IdFieldError):
            self._index(compact=True)

    def test_compaction_defaults_off(self):
        out = self._index()
        self.assertTrue(any(Path(out).rglob("*.parquet")))


class TestOverwriteRequired(TestRunthrough):
    """Requires a first successful run to create output, then checks the guard."""

    def test_overwrite_flag_required_on_second_run(self):
        h3(
            [
                TEST_FILE_PATH,
                str(self.output_path),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "8",
                "-t",
                "1",
            ],
            standalone_mode=False,
        )
        with self.assertRaises(FileExistsError):
            h3(
                [
                    TEST_FILE_PATH,
                    str(self.output_path),
                    "--layer",
                    TEST_LAYER_NAME,
                    "-r",
                    "8",
                ],
                standalone_mode=False,
            )


class TestStagedOutput(TestRunthrough):
    """A failed run must never cost the user existing output (-o), nor leave
    a half-written target that poisons the retry (fresh runs)."""

    def _run_h3(self, *extra):
        h3(
            [
                TEST_FILE_PATH,
                str(self.output_path),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "8",
                "-t",
                "1",
                *extra,
            ],
            standalone_mode=False,
        )

    def test_failed_overwrite_run_preserves_previous_output(self):
        self._run_h3()
        before = sorted(p.name for p in self.output_path.rglob("*.parquet"))
        self.assertTrue(before)
        with self.assertRaises(DataLayerError):
            self._run_h3("-o", "--layer", "no_such_layer")
        after = sorted(p.name for p in self.output_path.rglob("*.parquet"))
        self.assertEqual(before, after, "previous output was destroyed")

    def test_failed_fresh_run_leaves_no_output_dir(self):
        with self.assertRaises(DataLayerError):
            self._run_h3("--layer", "no_such_layer")
        self.assertFalse(self.output_path.exists(), "half-written target left behind")
        self._run_h3()  # retry must not require -o
        self.assertTrue(any(self.output_path.rglob("*.parquet")))
