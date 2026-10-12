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

    def test_no_stray_index_column(self):
        """No leaked "index" column, with or without an explicit id_field."""
        self._run_h3()
        self.assertNotIn("index", pq.read_table(self._parquet_files()[0]).schema.names)
        self._run_h3(("-id", "LCDB_UID", "-o"))
        self.assertNotIn("index", pq.read_table(self._parquet_files()[0]).schema.names)

    def test_default_id_uses_real_fid_not_synthetic(self):
        """
        No -id given, but the GPKG has a physically-stored FID column:
        auto-detected and used, rather than a fresh 0...n sequence. The
        fixture's real FIDs start at 1 (never 0).
        """
        self._run_h3()
        df = pq.read_table(self._parquet_files()[0]).to_pandas()
        self.assertIn("fid", df.columns)
        self.assertNotIn(0, set(df["fid"]))

    def test_compaction_without_explicit_id_uses_autodetected_fid(self):
        """-co works without -id, since a real FID is auto-detected to satisfy it."""
        self._run_h3(("-co",))
        table = pq.read_table(self._parquet_files()[0])
        self.assertIn("fid", table.schema.names)


class TestAttributeSchemaConsistency(TestRunthrough):
    """
    Kept-attribute columns must carry one Arrow type across every file in
    the store, even when a batch (and so a whole partition) is NULL
    throughout for that attribute (issue #227).
    """

    @classmethod
    def setUpClass(cls):
        skip_unless_backend("h3")
        super().setUpClass()

    def test_all_null_partition_matches_store_schema(self):
        import tempfile
        import warnings
        from pathlib import Path
        from unittest import mock

        import geopandas as gpd
        from shapely.geometry import Point

        import vector2dggs.constants as const
        from vector2dggs import common

        with tempfile.TemporaryDirectory() as d:
            # Two distant clusters -> distinct partition dirs; the first
            # cluster is NULL throughout for the kept attribute, and tiny
            # forced batches make its batches all-NULL, like a mostly-NULL
            # column over a many-batch national dataset.
            import pandas as pd

            a = [(174.0 + i * 0.01, -41.0, None) for i in range(40)]
            b = [
                (10.0 + i * 0.01, 50.0, "open" if i % 2 else "closed")
                for i in range(40)
            ]
            rows = a + b
            n = len(a)
            df = gpd.GeoDataFrame(
                {
                    "status": [r[2] for r in rows],
                    "lanes": pd.array(
                        [None] * n + [2**53 + 3 + i for i in range(n)], dtype="Int64"
                    ),
                    "oneway": pd.array(
                        [None] * n + [i % 2 == 0 for i in range(n)], dtype="boolean"
                    ),
                },
                geometry=[Point(r[0], r[1]) for r in rows],
                crs=4326,
            )
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                df.to_file(f"{d}/roads.gpkg", layer="roads")
            # a true OFTBinary field (geopandas writes bytes as TEXT):
            # a GPKG is SQLite, so add a BLOB column directly
            import sqlite3

            with sqlite3.connect(f"{d}/roads.gpkg") as sq:
                # the rtree triggers call SpatiaLite functions plain sqlite3
                # lacks; this test never uses spatial filtering
                for (name,) in sq.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'trigger'"
                    " AND name LIKE 'rtree_roads_%'"
                ).fetchall():
                    sq.execute(f"DROP TRIGGER {name}")
                sq.execute("ALTER TABLE roads ADD COLUMN payload BLOB")
                sq.execute(
                    "UPDATE roads SET payload = CAST('P' || fid AS BLOB)"
                    f" WHERE fid > {n}"
                )

            for compact in (False, True):
                with (
                    self.subTest(compact=compact),
                    mock.patch.object(const, "INGEST_PROBE_ROWS", 10),
                    mock.patch.object(const, "TARGET_BATCH_BYTES", 1),
                ):
                    out = f"{d}/out-{compact}.pq"
                    common.index(
                        "h3",
                        f"{d}/roads.gpkg",
                        out,
                        9,
                        None,
                        False,
                        1,
                        layer="roads",
                        keep_attribute=("status", "lanes", "oneway", "payload"),
                        compact=compact,
                    )
                    files = list(Path(out).rglob("*.parquet"))
                    for column, expected in (
                        (
                            "status",
                            {"dictionary<values=string, indices=int8, ordered=0>"},
                        ),
                        ("lanes", {"int64"}),
                        ("oneway", {"bool"}),
                        ("payload", {"binary"}),
                    ):
                        types = {
                            str(pq.read_schema(f).field(column).type) for f in files
                        }
                        self.assertEqual(types, expected, column)
                    # integers above 2**53 survive exactly: a float64 detour
                    # anywhere in the pipeline would round them
                    values = set()
                    for f in files:
                        values.update(
                            pq.read_table(f, columns=["lanes"])["lanes"]
                            .drop_null()
                            .to_pylist()
                        )
                    self.assertEqual(values, {2**53 + 3 + i for i in range(n)})

    def test_list_attribute_passes_through(self):
        # List-typed fields keep their per-batch typing for now, but must
        # still flow through the pipeline (nested Arrow dtypes cannot be
        # rebuilt from staged-file metadata by pandas)
        import json
        import tempfile
        from pathlib import Path
        from unittest import mock

        import vector2dggs.constants as const
        from vector2dggs import common

        with tempfile.TemporaryDirectory() as d:
            features = [
                {
                    "type": "Feature",
                    "properties": {"il": None if i < 40 else [1, i]},
                    "geometry": {
                        "type": "Point",
                        "coordinates": [
                            (174.0 if i < 40 else 10.0) + (i % 40) * 0.01,
                            -41.0 if i < 40 else 50.0,
                        ],
                    },
                }
                for i in range(80)
            ]
            with open(f"{d}/t.geojson", "w") as fh:
                json.dump({"type": "FeatureCollection", "features": features}, fh)

            with (
                mock.patch.object(const, "INGEST_PROBE_ROWS", 10),
                mock.patch.object(const, "TARGET_BATCH_BYTES", 1),
            ):
                common.index(
                    "h3",
                    f"{d}/t.geojson",
                    f"{d}/out.pq",
                    9,
                    None,
                    False,
                    1,
                    keep_attribute=("il",),
                    compact=False,
                )
            types = {
                str(pq.read_schema(f).field("il").type)
                for f in Path(f"{d}/out.pq").rglob("*.parquet")
            }
            self.assertTrue(any(t.startswith("list<") for t in types), types)
