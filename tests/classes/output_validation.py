import json

import pyarrow.parquet as pq

from vector2dggs.h3 import h3

from ..data.datapaths import TEST_FILE_PATH, TEST_LAYER_NAME
from .base import TestRunthrough, skip_unless_backend


class TestOutputValidation(TestRunthrough):
    """
    Reads output parquet files back after indexing and asserts structural
    correctness. Uses H3 at resolution 8 (default parent_res=2) as the
    reference backend throughout.
    """

    @classmethod
    def setUpClass(cls):
        skip_unless_backend("h3")
        super().setUpClass()

    def _parquet_files(self):
        files = sorted(self.output_path.rglob("*.parquet"))
        self.assertTrue(files, "No parquet files written to output")
        return files

    def _run_h3(self, extra_args=()):
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
                *extra_args,
            ],
            standalone_mode=False,
        )

    def test_partition_dirs_named_by_parent_res(self):
        """Hive partition directories are named h3_02=<token>."""
        self._run_h3()
        dirs = [d for d in self.output_path.iterdir() if d.is_dir()]
        self.assertTrue(dirs, "No partition directories in output")
        for d in dirs:
            self.assertTrue(
                d.name.startswith("h3_02="),
                f"Expected h3_02=… partition dir, got: {d.name}",
            )

    def test_explicit_parent_res_reflected_in_dirs(self):
        """--parent-res 3 produces h3_03=… partition directories."""
        self._run_h3(("-pr", "3"))
        dirs = [d for d in self.output_path.iterdir() if d.is_dir()]
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

    def test_keep_attribute_restricts_to_named_columns(self):
        """-ka limits output to exactly the requested attribute columns."""
        self._run_h3(("-ka", "LCDB_UID"))
        table = pq.read_table(self._parquet_files()[0])
        self.assertIn("LCDB_UID", table.schema.names)
        self.assertNotIn("Name_2018", table.schema.names)

    def test_keep_attribute_repeated_for_multiple_columns(self):
        """-ka can be repeated to keep several specific columns."""
        self._run_h3(("-ka", "LCDB_UID", "-ka", "Name_2018"))
        table = pq.read_table(self._parquet_files()[0])
        self.assertIn("LCDB_UID", table.schema.names)
        self.assertIn("Name_2018", table.schema.names)

    def test_keep_attribute_overrides_keep_attributes_flag(self):
        """-ka takes precedence over -k when both are given."""
        self._run_h3(("-k", "-ka", "LCDB_UID"))
        table = pq.read_table(self._parquet_files()[0])
        self.assertIn("LCDB_UID", table.schema.names)
        self.assertNotIn("Name_2018", table.schema.names)
