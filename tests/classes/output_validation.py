import json

import pyarrow.parquet as pq

from .base import TestRunthrough
from ..data.datapaths import *

from vector2dggs.h3 import h3


class TestOutputValidation(TestRunthrough):
    """
    Reads output parquet files back after indexing and asserts structural
    correctness. Uses H3 at resolution 8 (default parent_res=2) as the
    reference backend throughout.
    """

    def _parquet_files(self):
        files = sorted(TEST_OUTPUT_PATH.rglob("*.parquet"))
        self.assertTrue(files, "No parquet files written to output")
        return files

    def _run_h3(self, extra_args=()):
        h3(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "8",
                *extra_args,
            ],
            standalone_mode=False,
        )

    def test_partition_dirs_named_by_parent_res(self):
        """Hive partition directories are named h3_02=<token>."""
        self._run_h3()
        dirs = [d for d in TEST_OUTPUT_PATH.iterdir() if d.is_dir()]
        self.assertTrue(dirs, "No partition directories in output")
        for d in dirs:
            self.assertTrue(
                d.name.startswith("h3_02="),
                f"Expected h3_02=… partition dir, got: {d.name}",
            )

    def test_explicit_parent_res_reflected_in_dirs(self):
        """--parent-res 3 produces h3_03=… partition directories."""
        self._run_h3(("-pr", "3"))
        dirs = [d for d in TEST_OUTPUT_PATH.iterdir() if d.is_dir()]
        self.assertTrue(dirs, "No partition directories in output")
        for d in dirs:
            self.assertTrue(
                d.name.startswith("h3_03="),
                f"Expected h3_03=… partition dir, got: {d.name}",
            )

    def test_geo_point_output_has_geometry_column(self):
        """GeoParquet point output contains a geometry column."""
        self._run_h3(("--geo", "point"))
        table = pq.read_table(self._parquet_files()[0])
        self.assertIn("geometry", table.schema.names)

    def test_geo_point_output_has_geoparquet_metadata(self):
        """GeoParquet point output carries valid geo metadata."""
        self._run_h3(("--geo", "point"))
        table = pq.read_table(self._parquet_files()[0])
        self.assertIn(b"geo", table.schema.metadata)
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["primary_column"], "geometry")
        self.assertIn("geometry", geo["columns"])

    def test_geo_polygon_output_has_geometry_column(self):
        """GeoParquet polygon output contains a geometry column."""
        self._run_h3(("--geo", "polygon"))
        table = pq.read_table(self._parquet_files()[0])
        self.assertIn("geometry", table.schema.names)

    def test_keep_attributes_retains_source_columns(self):
        """--keep-attributes includes original attribute columns in output."""
        self._run_h3(("-k",))
        table = pq.read_table(self._parquet_files()[0])
        self.assertIn("Name_2018", table.schema.names)
        self.assertIn("LCDB_UID", table.schema.names)
