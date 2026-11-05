import unittest

import geopandas as gpd
import pyarrow as pa
import pyarrow.parquet as pq
import sqlalchemy

from vector2dggs.h3 import h3

from ..data.datapaths import TEST_FILE_PATH, TEST_LAYER_NAME
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
                str(self.output_path),
                "--layer",
                self.TABLE_NAME,
                "-g",
                "geometry",
                "-r",
                "8",
                "-t",
                "1",
            ],
            standalone_mode=False,
        )
        files = sorted(self.output_path.rglob("*.parquet"))
        self.assertTrue(files, "No parquet files written from PostGIS input")
        table = pq.read_table(files[0])
        self.assertIn("h3_08", table.schema.names)

    def test_postgis_keep_attributes(self):
        """--keep_attributes selects every column, not just id_field + geom."""
        h3(
            [
                self.connection_url,
                str(self.output_path),
                "--layer",
                self.TABLE_NAME,
                "-g",
                "geometry",
                "-r",
                "8",
                "-t",
                "1",
                "-k",
            ],
            standalone_mode=False,
        )
        files = sorted(self.output_path.rglob("*.parquet"))
        self.assertTrue(files, "No parquet files written from PostGIS input")
        table = pq.read_table(files[0])
        self.assertIn("Name_2018", table.schema.names)

    def test_postgis_id_field(self):
        """-id selects id_field + geom_col only (the non-keep_attributes default path)."""
        h3(
            [
                self.connection_url,
                str(self.output_path),
                "--layer",
                self.TABLE_NAME,
                "-g",
                "geometry",
                "-id",
                "LCDB_UID",
                "-r",
                "8",
                "-t",
                "1",
            ],
            standalone_mode=False,
        )
        files = sorted(self.output_path.rglob("*.parquet"))
        self.assertTrue(files, "No parquet files written from PostGIS input")
        table = pq.read_table(files[0])
        self.assertIn("LCDB_UID", table.schema.names)
        self.assertNotIn("Name_2018", table.schema.names)

    def _table_with_primary_key(
        self, table_name: str, extra_pk_columns: tuple[str, ...] = ()
    ) -> None:
        """
        Writes the fixture with its pandas RangeIndex as a "row_pk" column
        (genuinely unique by construction, unlike LCDB_UID: several source
        rows legitimately share one feature id), then makes that column
        (plus any extra_pk_columns, for a composite key) the table's PK.
        """
        engine = sqlalchemy.create_engine(self.connection_url)
        try:
            gdf = gpd.read_file(TEST_FILE_PATH, layer=TEST_LAYER_NAME)
            gdf.to_postgis(
                table_name,
                engine,
                if_exists="replace",
                index=True,
                index_label="row_pk",
            )
            cols = ", ".join(f'"{c}"' for c in ("row_pk", *extra_pk_columns))
            with engine.begin() as connection:
                connection.execute(
                    sqlalchemy.text(
                        f'ALTER TABLE "{table_name}" ADD PRIMARY KEY ({cols})'
                    )
                )
        finally:
            engine.dispose()

    def test_postgis_default_id_uses_primary_key(self):
        """No -id given, but the table has a single-column PK: auto-detected
        and used, rather than falling back to a synthetic fid."""
        table_name = "vector2dggs_test_pk_table"
        self._table_with_primary_key(table_name)
        h3(
            [
                self.connection_url,
                str(self.output_path),
                "--layer",
                table_name,
                "-g",
                "geometry",
                "-r",
                "8",
                "-t",
                "1",
            ],
            standalone_mode=False,
        )
        files = sorted(self.output_path.rglob("*.parquet"))
        self.assertTrue(files, "No parquet files written from PostGIS input")
        table = pq.read_table(files[0])
        self.assertIn("row_pk", table.schema.names)
        self.assertNotIn("fid", table.schema.names)

    def _run_h3(self, table_name: str, extra: tuple[str, ...] = ()) -> pa.Table:
        h3(
            [
                self.connection_url,
                str(self.output_path),
                "--layer",
                table_name,
                "-g",
                "geometry",
                "-r",
                "8",
                "-t",
                "1",
                *extra,
            ],
            standalone_mode=False,
        )
        files = sorted(self.output_path.rglob("*.parquet"))
        self.assertTrue(files, "No parquet files written from PostGIS input")
        return pq.read_table(files[0])

    def test_postgis_composite_primary_key_is_auto_detected(self):
        """Every column of a composite PK is used, in key order (#194)."""
        table_name = "vector2dggs_test_composite_pk_table"
        self._table_with_primary_key(table_name, extra_pk_columns=("LCDB_UID",))
        table = self._run_h3(table_name)
        names = table.schema.names
        self.assertNotIn("fid", names)
        self.assertLess(names.index("row_pk"), names.index("LCDB_UID"))

    def test_postgis_composite_id_field(self):
        """An explicit composite -id over DB input, output in the order given."""
        table_name = "vector2dggs_test_composite_id_table"
        self._table_with_primary_key(table_name)
        table = self._run_h3(table_name, ("-id", "LCDB_UID", "-id", "row_pk"))
        names = table.schema.names
        self.assertLess(names.index("LCDB_UID"), names.index("row_pk"))
        self.assertNotIn("fid", names)
