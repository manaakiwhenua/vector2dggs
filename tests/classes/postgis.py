import unittest

import geopandas as gpd
import pyarrow.parquet as pq
import sqlalchemy

from vector2dggs.h3 import h3

from ..data.datapaths import TEST_FILE_PATH, TEST_LAYER_NAME, TEST_OUTPUT_PATH
from .base import TestRunthrough


class TestPostGIS(TestRunthrough):
    """
    Exercises the database-connection input path (common._read_input's
    con/layer branch), which every other test in this suite bypasses by
    reading directly from a file.

    Spins up a throwaway PostGIS container via testcontainers. Skips
    (rather than fails) if Docker isn't reachable, so this doesn't break
    the suite for contributors without Docker running locally.
    """

    TABLE_NAME = "vector2dggs_test_layer"

    @classmethod
    def setUpClass(cls):
        try:
            from testcontainers.community.postgres import PostgresContainer
        except Exception as e:
            raise unittest.SkipTest(f"testcontainers not available: {e}") from e

        try:
            cls.pg = PostgresContainer("postgis/postgis:16-3.4")
            cls.pg.start()
        except Exception as e:
            raise unittest.SkipTest(
                f"Docker unavailable, skipping PostGIS tests: {e}"
            ) from e

        cls.connection_url = cls.pg.get_connection_url()
        engine = sqlalchemy.create_engine(cls.connection_url)
        try:
            gdf = gpd.read_file(TEST_FILE_PATH, layer=TEST_LAYER_NAME)
            gdf.to_postgis(cls.TABLE_NAME, engine, if_exists="replace", index=False)
        finally:
            engine.dispose()

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "pg"):
            cls.pg.stop()

    def test_postgis_run(self):
        """Default invocation: geometry-only select, synthetic fid index."""
        h3(
            [
                self.connection_url,
                str(TEST_OUTPUT_PATH),
                "--layer",
                self.TABLE_NAME,
                "-g",
                "geometry",
                "-r",
                "8",
            ],
            standalone_mode=False,
        )
        files = sorted(TEST_OUTPUT_PATH.rglob("*.parquet"))
        self.assertTrue(files, "No parquet files written from PostGIS input")
        table = pq.read_table(files[0])
        self.assertIn("h3_08", table.schema.names)

    def test_postgis_keep_attributes(self):
        """--keep_attributes selects every column, not just id_field + geom."""
        h3(
            [
                self.connection_url,
                str(TEST_OUTPUT_PATH),
                "--layer",
                self.TABLE_NAME,
                "-g",
                "geometry",
                "-r",
                "8",
                "-k",
            ],
            standalone_mode=False,
        )
        files = sorted(TEST_OUTPUT_PATH.rglob("*.parquet"))
        self.assertTrue(files, "No parquet files written from PostGIS input")
        table = pq.read_table(files[0])
        self.assertIn("Name_2018", table.schema.names)

    def test_postgis_id_field(self):
        """-id selects id_field + geom_col only (the non-keep_attributes default path)."""
        h3(
            [
                self.connection_url,
                str(TEST_OUTPUT_PATH),
                "--layer",
                self.TABLE_NAME,
                "-g",
                "geometry",
                "-id",
                "LCDB_UID",
                "-r",
                "8",
            ],
            standalone_mode=False,
        )
        files = sorted(TEST_OUTPUT_PATH.rglob("*.parquet"))
        self.assertTrue(files, "No parquet files written from PostGIS input")
        table = pq.read_table(files[0])
        self.assertIn("LCDB_UID", table.schema.names)
        self.assertNotIn("Name_2018", table.schema.names)
