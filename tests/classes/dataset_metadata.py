import importlib
import json
from pathlib import Path

import pyarrow.parquet as pq

from vector2dggs import __version__, common
from vector2dggs.indexerfactory import indexer_instance

from ..data.datapaths import TEST_FILE_PATH, TEST_LAYER_NAME
from .base import TestRunthrough, skip_unless_backend

OGC_URI = "https://www.opengis.net/def/dggrs/OGC/1.0/rHEALPix"
BACKENDS = ("h3", "rhp", "s2", "a5", "geohash")
RES = {"h3": 8, "s2": 13, "a5": 15, "rhp": 8, "geohash": 6}
MODULES = {"rhp": "rHP"}


def _installed():
    for dggs in BACKENDS:
        try:
            skip_unless_backend(dggs)
        except Exception:
            continue
        yield dggs


def _metadata(file: Path) -> tuple[dict, dict]:
    schema = pq.read_schema(file)
    meta = json.loads((schema.metadata or {})[b"dggrs"])
    cell = schema.field(meta["cell_column"]).metadata or {}
    return meta, {k.decode(): v.decode() for k, v in cell.items()}


class TestDatasetMetadata(TestRunthrough):
    """
    Every output file says which grid its cell IDs belong to (#244): a
    "dggrs" schema key, and the grid on the cell column's own field.
    """

    def _run(self, dggs: str, out: Path, extra=()) -> list[Path]:
        module = importlib.import_module(f"vector2dggs.{MODULES.get(dggs, dggs)}")
        getattr(module, dggs)(
            [
                TEST_FILE_PATH,
                str(out),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                str(RES[dggs]),
                "-t",
                "1",
                *extra,
            ],
            standalone_mode=False,
        )
        files = sorted(out.rglob("*.parquet"))
        self.assertTrue(files, f"{dggs}: no parquet output written")
        return files

    def test_every_backend_and_file_carries_the_key(self):
        for dggs in _installed():
            with self.subTest(dggs=dggs):
                for file in self._run(dggs, self.output_path / dggs):
                    meta, cell = _metadata(file)
                    self.assertEqual(meta["dggs"], dggs)
                    self.assertEqual(meta["resolution"], RES[dggs])
                    self.assertEqual(meta["cell_column"], f"{dggs}_{RES[dggs]:02}")
                    self.assertEqual(meta["mode"], "centre")
                    self.assertFalse(meta["compact"])
                    self.assertEqual(meta["vector2dggs"], __version__)
                    for lib, ver in meta["libraries"].items():
                        self.assertIsNotNone(ver, f"{lib} version missing")
                    self.assertEqual(cell["dggs"], dggs)
                    self.assertEqual(cell["resolution"], str(RES[dggs]))

    def test_rhp_codespace_follows_the_grid(self):
        skip_unless_backend("rhp")
        (ogc,) = self._run("rhp", self.output_path / "ogc")
        meta, cell = _metadata(ogc)
        self.assertEqual((meta["codespace"], meta["grid"]), (OGC_URI, OGC_URI))
        self.assertEqual(cell["dggrs"], OGC_URI)
        self.assertEqual(meta["definition"]["uri"], OGC_URI)
        (std,) = self._run("rhp", self.output_path / "std", ("--grid", "003"))
        meta, cell = _metadata(std)
        self.assertIsNone(meta["codespace"])
        self.assertEqual((meta["grid"], cell["dggrs"]), ("003", "003"))
        self.assertNotIn("uri", meta["definition"])

    def test_custom_grid_parameters_are_in_the_definition(self):
        skip_unless_backend("rhp")
        (file,) = self._run("rhp", self.output_path / "custom", ("--grid", "123@50"))
        meta, cell = _metadata(file)
        self.assertIsNone(meta["codespace"])
        self.assertEqual((meta["grid"], cell["dggrs"]), ("123@50", "123@50"))
        params = {
            p["name"]: p["value"]
            for p in meta["definition"]["dggh"]["definition"]["crs"]["conversion"][
                "parameters"
            ]
        }
        self.assertEqual(
            (params["lon_0"], params["north_square"], params["south_square"]),
            (50, 1, 2),
        )

    def test_survives_compaction_and_sits_beside_geoparquet(self):
        skip_unless_backend("h3")
        (file,) = self._run("h3", self.output_path / "co", ("-co", "--geo", "point"))
        schema = pq.read_schema(file)
        self.assertIn(b"geo", schema.metadata)
        meta, cell = _metadata(file)
        self.assertTrue(meta["compact"])
        self.assertEqual(cell["dggs"], "h3")

    def test_empty_output_carries_it_too(self):
        skip_unless_backend("h3")
        (file,) = self._run("h3", self.output_path / "empty", ("-r", "0"))
        self.assertEqual(pq.read_metadata(file).num_rows, 0)
        meta, cell = _metadata(file)
        self.assertEqual(meta["resolution"], 0)
        self.assertEqual(cell["resolution"], "0")

    def test_library_path_matches(self):
        skip_unless_backend("h3")
        out = self.output_path / "lib"
        common.index(
            "h3", TEST_FILE_PATH, out, 8, None, False, 1, layer=TEST_LAYER_NAME
        )
        meta, _ = _metadata(next(out.rglob("*.parquet")))
        self.assertEqual(meta["dggs"], "h3")
        self.assertEqual(indexer_instance("h3").library_versions(), meta["libraries"])
