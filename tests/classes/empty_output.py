import json
import tempfile
import warnings
from pathlib import Path
from unittest import TestCase

import geopandas as gpd
import pandas as pd
import pyarrow.parquet as pq
from shapely.geometry import Polygon

from vector2dggs import common

from .base import TestRunthrough, skip_unless_backend

# Far smaller than a cell at the resolution below, so no cell centre falls
# inside it and the run legitimately indexes nothing
TINY = Polygon(
    [
        (174.70000, -41.30000),
        (174.70020, -41.30000),
        (174.70020, -41.29980),
        (174.70000, -41.29980),
    ]
)
RES, PARENT_RES = 5, 2


class TestEmptyOutput(TestRunthrough):
    """
    A run that indexes nothing still has to produce something a reader can
    open. The result is a legitimate answer - usually that the resolution
    is too coarse - and an empty directory is indistinguishable from a
    failed run, so it is written as an empty dataset carrying the schema
    the run would have produced.
    """

    def setUp(self):
        super().setUp()
        skip_unless_backend("h3")
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.input = Path(tmp.name) / "tiny.gpkg"
        frame = gpd.GeoDataFrame({"name": ["a"], "geometry": [TINY]}, crs=4326)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            frame.to_file(self.input, layer="tiny")

    def _index(self, **kwargs):
        common.index(
            "h3",
            self.input,
            self.output_path,
            RES,
            str(PARENT_RES),
            kwargs.pop("keep_attributes", False),
            1,
            layer="tiny",
            **kwargs,
        )
        return sorted(Path(self.output_path).rglob("*.parquet"))

    def test_writes_a_readable_empty_dataset(self):
        files = self._index()
        self.assertTrue(files, "no output written at all")
        df = pd.read_parquet(self.output_path)
        self.assertEqual(len(df), 0)
        self.assertEqual(df.index.name, f"h3_{RES:02}")
        self.assertIn(f"h3_{PARENT_RES:02}", df.columns)

    def test_warns_that_nothing_was_indexed(self):
        with self.assertLogs(common.LOGGER, level="WARNING") as logs:
            self._index()
        self.assertTrue(
            any("may be too coarse" in message for message in logs.output),
            logs.output,
        )

    def test_geoparquet_metadata_is_present(self):
        files = self._index(geo="polygon")
        table = pq.read_table(files[0])
        self.assertIn(b"geo", table.schema.metadata or {})
        meta = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(meta["version"], "1.1.0")
        self.assertEqual(meta["primary_column"], "geometry")
        # geopandas must accept it, not merely pyarrow
        self.assertEqual(len(gpd.read_parquet(self.output_path)), 0)

    def test_attributes_are_carried_into_the_schema(self):
        """
        The point of writing the schema is that a consumer can read it; an
        empty dataset missing the columns it would have had is only
        marginally better than no dataset.
        """
        self._index(keep_attributes=True)
        self.assertIn("name", pd.read_parquet(self.output_path).columns)

    def test_empty_is_empty_whichever_mode_asked(self):
        """
        centre indexes nothing here and stops before the merge; within
        reaches the merge, because a feature's boundary linework produces
        cells of its own before they are subtracted again. Both have to
        leave the same readable, empty result.
        """
        for mode in ("centre", "within"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as d:
                common.index(
                    "h3",
                    self.input,
                    f"{d}/out.pq",
                    RES,
                    str(PARENT_RES),
                    False,
                    1,
                    layer="tiny",
                    mode=mode,
                )
                self.assertTrue(sorted(Path(f"{d}/out.pq").rglob("*.parquet")))
                df = pd.read_parquet(f"{d}/out.pq")
                self.assertEqual(len(df), 0)
                self.assertEqual(df.index.name, f"h3_{RES:02}")


class TestEmptyOutputSchemaMatchesPopulated(TestCase):
    """
    The empty dataset has to be the schema the run would have produced, or
    it is a different dataset that happens to have no rows.

    Compared as a reader sees it rather than file against file, because the
    two cannot be identical on disk: in a populated run the parent column
    is a hive key, stored in the directory name and not in any file, and an
    empty run has no directories to put it in. It is carried as data
    instead, typed so that it reads back the same way.
    """

    # a whole degree square, so it comfortably contains cell centres at
    # the same coarse resolution that indexes TINY to nothing
    BIG = Polygon([(174.0, -41.5), (175.0, -41.5), (175.0, -40.5), (174.0, -40.5)])

    def setUp(self):
        skip_unless_backend("h3")

    def _read(self, source, layer, out, **kwargs):
        common.index(
            "h3",
            source,
            out,
            RES,
            str(PARENT_RES),
            kwargs.pop("keep_attributes", False),
            1,
            layer=layer,
            **kwargs,
        )
        return pd.read_parquet(out)

    def _assert_schemas_match(self, **kwargs):
        with tempfile.TemporaryDirectory() as d:
            source = Path(d) / "shapes.gpkg"
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                for layer, geom in (("tiny", TINY), ("big", self.BIG)):
                    gpd.GeoDataFrame(
                        {"name": ["a"], "geometry": [geom]}, crs=4326
                    ).to_file(source, layer=layer)
            empty = self._read(source, "tiny", f"{d}/empty.pq", **dict(kwargs))
            populated = self._read(source, "big", f"{d}/populated.pq", **dict(kwargs))

        self.assertEqual(len(empty), 0)
        self.assertGreater(len(populated), 0)
        self.assertEqual(empty.index.name, populated.index.name)
        self.assertEqual(list(empty.columns), list(populated.columns))
        self.assertEqual(
            {c: str(empty[c].dtype) for c in empty.columns},
            {c: str(populated[c].dtype) for c in populated.columns},
        )

    def test_default(self):
        self._assert_schemas_match()

    def test_geo_polygon(self):
        self._assert_schemas_match(geo="polygon")

    def test_kept_attributes(self):
        self._assert_schemas_match(keep_attributes=True)

    def test_cell_id_uint64(self):
        """uint64 cell ids round-trip exactly, index dtype included."""
        self._assert_schemas_match(cell_id="uint64")
        with tempfile.TemporaryDirectory() as d:
            source = Path(d) / "shapes.gpkg"
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                for layer, geom in (("tiny", TINY), ("big", self.BIG)):
                    gpd.GeoDataFrame(
                        {"name": ["a"], "geometry": [geom]}, crs=4326
                    ).to_file(source, layer=layer)
            empty = self._read(source, "tiny", f"{d}/e.pq", cell_id="uint64")
            populated = self._read(source, "big", f"{d}/p.pq", cell_id="uint64")
        self.assertEqual(str(empty.index.dtype), str(populated.index.dtype))

    def test_parent_column_reads_back_as_a_category(self):
        """
        Pinning the one type that cannot be inferred from the empty frame:
        a populated run's parent column is a hive key that readers rebuild
        as a dictionary, so the empty one is cast to match. Left as a
        plain string it would read back as a different dtype.
        """
        with tempfile.TemporaryDirectory() as d:
            source = Path(d) / "tiny.gpkg"
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                gpd.GeoDataFrame({"name": ["a"], "geometry": [TINY]}, crs=4326).to_file(
                    source, layer="tiny"
                )
            empty = self._read(source, "tiny", f"{d}/out.pq")
        self.assertEqual(
            str(empty[f"h3_{PARENT_RES:02}"].dtype),
            "category",
        )
