import tempfile
import zipfile
from pathlib import Path
from unittest import TestCase, mock

from pyogrio.errors import DataSourceError

from vector2dggs import common

from ..data.datapaths import TEST_FILE_PATH


class TestInputDispatch(TestCase):
    """db_conn_and_input_path: file if it exists on disk, database if
    sqlalchemy can engine it, anything else GDAL can open is passed through;
    GDAL's DataSourceError propagates otherwise."""

    def test_existing_file_returned_as_path(self):
        with tempfile.NamedTemporaryFile(suffix=".gpkg") as f:
            con, path = common.db_conn_and_input_path(f.name)
        self.assertIsNone(con)
        self.assertEqual(path, Path(f.name))

    def test_postgres_url_returns_engine(self):
        con, path = common.db_conn_and_input_path(
            "postgresql+psycopg2://user:pw@localhost:1/db"
        )
        self.assertIsNotNone(con)

    def test_missing_postgres_driver_gives_install_hint(self):
        with mock.patch.object(
            common.sqlalchemy,
            "create_engine",
            side_effect=ModuleNotFoundError("No module named 'psycopg2'"),
        ), self.assertRaisesRegex(ImportError, r"vector2dggs\[postgres\]"):
            common.db_conn_and_input_path("postgresql+psycopg2://u:p@localhost:1/db")

    def test_gdal_virtual_path_passed_through(self):
        with tempfile.TemporaryDirectory() as d:
            zip_path = f"{d}/data.zip"
            with zipfile.ZipFile(zip_path, "w") as z:
                z.write(TEST_FILE_PATH, arcname="se-island.gpkg")
            vsi = f"/vsizip/{zip_path}/se-island.gpkg"
            con, path = common.db_conn_and_input_path(vsi)
            self.assertIsNone(con)
            self.assertEqual(path, vsi)

    def test_windows_drive_letter_path_not_treated_as_database(self):
        with self.assertRaises(DataSourceError):
            common.db_conn_and_input_path(r"C:\data\input.gpkg")

    def test_nonexistent_plain_path_raises(self):
        with self.assertRaises(DataSourceError):
            common.db_conn_and_input_path("/nonexistent/data.gpkg")

    def test_nonexistent_uri_raises(self):
        with self.assertRaises(DataSourceError):
            common.db_conn_and_input_path("file:///nonexistent/data.gpkg")
