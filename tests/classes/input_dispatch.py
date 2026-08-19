import tempfile
from pathlib import Path
from unittest import TestCase

from vector2dggs import common


class TestInputDispatch(TestCase):
    """db_conn_and_input_path: file if it exists, database if sqlalchemy can
    engine it, remote pass-through if it has a scheme, else FileNotFoundError."""

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

    def test_windows_drive_letter_path_not_treated_as_database(self):
        with self.assertRaises(FileNotFoundError):
            common.db_conn_and_input_path(r"C:\data\input.gpkg")

    def test_remote_uri_passed_through(self):
        con, path = common.db_conn_and_input_path("https://example.com/data.gpkg")
        self.assertIsNone(con)
        self.assertEqual(path, "https://example.com/data.gpkg")

    def test_file_uri_passed_through(self):
        con, path = common.db_conn_and_input_path("file:///nonexistent/data.gpkg")
        self.assertIsNone(con)
        self.assertEqual(path, "file:///nonexistent/data.gpkg")

    def test_nonexistent_plain_path_raises(self):
        with self.assertRaises(FileNotFoundError):
            common.db_conn_and_input_path("/nonexistent/data.gpkg")
