import math
import tempfile
from pathlib import Path
from unittest import TestCase, mock

import geopandas as gpd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
from shapely.geometry import Point, box

import vector2dggs.constants as const
from vector2dggs import common
from vector2dggs.indexers.h3vectorindexer import H3VectorIndexer

from ..data.datapaths import TEST_FILE_PATH, TEST_LAYER_NAME


class TestGetParentRes(TestCase):
    def test_explicit_parent_passes_through(self):
        self.assertEqual(common.get_parent_res("h3", "5", 9), 5)

    def test_default_derived_from_resolution(self):
        self.assertEqual(
            common.get_parent_res("h3", None, 9),
            const.DEFAULT_DGGS_PARENT_RES["h3"](9),
        )

    def test_unknown_dggs_raises(self):
        with self.assertRaises(RuntimeError):
            common.get_parent_res("not_a_dggs", None, 9)


class TestAvailableMemoryMb(TestCase):
    def test_parses_mem_available_line(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "meminfo"
            path.write_text(
                "MemTotal:       16384000 kB\nMemAvailable:    8192000 kB\n"
            )
            self.assertEqual(const._available_memory_mb(str(path)), 8000)

    def test_missing_file_returns_none(self):
        self.assertIsNone(const._available_memory_mb("/no/such/file"))

    def test_missing_mem_available_line_returns_none(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "meminfo"
            path.write_text("MemTotal:       16384000 kB\n")
            self.assertIsNone(const._available_memory_mb(str(path)))


class TestDefaultThreads(TestCase):
    """
    See issue #183: --threads defaulted to CPU count alone, with no regard
    for available memory, which could overcommit a memory-thin machine
    regardless of how lean each worker task was.
    """

    def test_falls_back_to_cpu_count_when_memory_unknown(self):
        with (
            mock.patch.object(const.multiprocessing, "cpu_count", return_value=9),
            mock.patch.object(const, "_available_memory_mb", return_value=None),
        ):
            self.assertEqual(const.default_threads(), 8)

    def test_caps_by_available_memory(self):
        with (
            mock.patch.object(const.multiprocessing, "cpu_count", return_value=32),
            mock.patch.object(const, "_available_memory_mb", return_value=2048),
            mock.patch.object(const, "RESERVED_MB_PER_WORKER", 512),
            mock.patch.object(const, "AVAILABLE_MEMORY_BUDGET_FRACTION", 1.0),
        ):
            self.assertEqual(const.default_threads(), 4)

    def test_budget_fraction_reduces_effective_cap(self):
        # Only a fraction of available memory is budgeted, leaving headroom
        # for other processes on the machine and for this one-shot-at-
        # startup reading to hold for a run lasting several minutes.
        with (
            mock.patch.object(const.multiprocessing, "cpu_count", return_value=32),
            mock.patch.object(const, "_available_memory_mb", return_value=2048),
            mock.patch.object(const, "RESERVED_MB_PER_WORKER", 512),
            mock.patch.object(const, "AVAILABLE_MEMORY_BUDGET_FRACTION", 0.5),
        ):
            self.assertEqual(const.default_threads(), 2)

    def test_never_goes_below_one(self):
        with (
            mock.patch.object(const.multiprocessing, "cpu_count", return_value=32),
            mock.patch.object(const, "_available_memory_mb", return_value=10),
            mock.patch.object(const, "RESERVED_MB_PER_WORKER", 512),
        ):
            self.assertEqual(const.default_threads(), 1)

    def test_memory_cap_never_exceeds_cpu_based_default(self):
        with (
            mock.patch.object(const.multiprocessing, "cpu_count", return_value=4),
            mock.patch.object(const, "_available_memory_mb", return_value=1_000_000),
            mock.patch.object(const, "RESERVED_MB_PER_WORKER", 512),
        ):
            self.assertEqual(const.default_threads(), 3)


class TestCheckRequestedAttributes(TestCase):
    def test_no_attributes_requested_is_a_noop(self):
        common.check_requested_attributes((), "/no/such/file.gpkg", None, None)

    def test_known_column_passes(self):
        common.check_requested_attributes(
            ("LCDB_UID",), TEST_FILE_PATH, TEST_LAYER_NAME, None
        )

    def test_unknown_column_raises(self):
        with self.assertRaises(common.UnknownAttributeError):
            common.check_requested_attributes(
                ("not_a_real_column",), TEST_FILE_PATH, TEST_LAYER_NAME, None
            )


class TestPrepareDataframeKeepAttribute(TestCase):
    def test_keep_attribute_overrides_keep_attributes_false(self):
        gdf = gpd.GeoDataFrame({"geometry": [Point(0, 0)], "a": [1], "b": [2]})
        result = common._prepare_dataframe(gdf, None, False, keep_attribute=("a",))
        self.assertEqual(sorted(result.columns), ["a", "geometry"])

    def test_keep_attribute_ignores_columns_not_present(self):
        gdf = gpd.GeoDataFrame({"geometry": [Point(0, 0)], "a": [1]})
        result = common._prepare_dataframe(
            gdf, None, False, keep_attribute=("a", "missing")
        )
        self.assertEqual(sorted(result.columns), ["a", "geometry"])

    def test_no_keep_attribute_falls_back_to_boolean(self):
        gdf = gpd.GeoDataFrame({"geometry": [Point(0, 0)], "a": [1]})
        kept_all = common._prepare_dataframe(gdf.copy(), None, True)
        self.assertIn("a", kept_all.columns)
        kept_none = common._prepare_dataframe(gdf.copy(), None, False)
        self.assertNotIn("a", kept_none.columns)

    def test_keep_attribute_columns_are_dictionary_encoded(self):
        gdf = gpd.GeoDataFrame({"geometry": [Point(0, 0)], "a": ["x"], "b": ["y"]})
        result = common._prepare_dataframe(gdf, None, False, keep_attribute=("a",))
        self.assertEqual(result["a"].dtype, "category")


class TestReadBatchesColumnSelection(TestCase):
    def test_keep_attribute_limits_columns_read(self):
        batch = next(
            common._read_batches(
                TEST_FILE_PATH,
                TEST_LAYER_NAME,
                None,
                False,
                None,
                "geometry",
                10,
                keep_attribute=("LCDB_UID",),
            )
        )
        self.assertIn("LCDB_UID", batch.columns)
        self.assertNotIn("Name_2018", batch.columns)

    def test_default_keep_attributes_false_only_reads_geometry(self):
        batch = next(
            common._read_batches(
                TEST_FILE_PATH, TEST_LAYER_NAME, None, False, None, "geometry", 10
            )
        )
        self.assertEqual(list(batch.columns), ["geometry"])


class TestFidColumnIdField(TestCase):
    """
    Reproduces issue #191: Kart working copies (and any GPKG built with a
    custom FID column name) promote the dataset's own PK to the GPKG
    FID slot rather than exposing it as a regular field. -id/--id_field
    pointing at that slot needs fid_as_index, since it's absent from both
    pyogrio's "fields" list and a plain columns= read.
    """

    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.gpkg_path = str(Path(self._tmpdir.name) / "kart_style.gpkg")
        gdf = gpd.GeoDataFrame(
            {
                "facility_id": [101, 102, 103],
                "name": ["a", "b", "c"],
                "geometry": [Point(0, 0), Point(1, 1), Point(2, 2)],
            },
            crs="EPSG:4326",
        )
        pyogrio.write_dataframe(
            gdf,
            self.gpkg_path,
            layer="facilities",
            layer_options={"FID": "facility_id"},
        )

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_fid_column_is_detected(self):
        self.assertEqual(
            common._fid_column(self.gpkg_path, "facilities"), "facility_id"
        )

    def test_fid_column_excluded_from_fields(self):
        info = pyogrio.read_info(self.gpkg_path, layer="facilities")
        self.assertNotIn("facility_id", info["fields"])

    def test_check_id_field_accepts_fid_column(self):
        common.check_id_field("facility_id", self.gpkg_path, "facilities", None)

    def test_check_id_field_accepts_regular_field(self):
        common.check_id_field("name", self.gpkg_path, "facilities", None)

    def test_check_id_field_rejects_unknown_field(self):
        with self.assertRaises(common.IdFieldError):
            common.check_id_field(
                "not_a_real_column", self.gpkg_path, "facilities", None
            )

    def test_read_batches_recovers_fid_column_values(self):
        batch = next(
            common._read_batches(
                self.gpkg_path,
                "facilities",
                None,
                False,
                "facility_id",
                "geometry",
                3,
            )
        )
        self.assertIn("facility_id", batch.columns)
        self.assertEqual(sorted(batch["facility_id"]), [101, 102, 103])

    def test_read_batches_regular_id_field_unaffected(self):
        batch = next(
            common._read_batches(
                self.gpkg_path,
                "facilities",
                None,
                False,
                "name",
                "geometry",
                3,
            )
        )
        self.assertIn("name", batch.columns)

    def test_prepare_dataframe_indexes_by_recovered_fid(self):
        batch = next(
            common._read_batches(
                self.gpkg_path,
                "facilities",
                None,
                False,
                "facility_id",
                "geometry",
                3,
            )
        )
        result = common._prepare_dataframe(batch, "facility_id", False)
        self.assertEqual(result.index.name, "facility_id")
        self.assertEqual(sorted(result.index), [101, 102, 103])

    def test_resolve_default_id_field_uses_fid_column(self):
        self.assertEqual(
            common.resolve_default_id_field(self.gpkg_path, "facilities", None),
            "facility_id",
        )


class TestResolveDefaultIdFieldNoFid(TestCase):
    """
    Formats without a physically-stored FID (e.g. Shapefile) have nothing
    for resolve_default_id_field to auto-detect: it falls back to None
    (the caller's synthetic-sequence path), with a warning logged so
    users understand IDs won't be stable across runs.
    """

    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.shp_path = str(Path(self._tmpdir.name) / "no_fid.shp")
        gdf = gpd.GeoDataFrame(
            {"name": ["a", "b"], "geometry": [Point(0, 0), Point(1, 1)]},
            crs="EPSG:4326",
        )
        gdf.to_file(self.shp_path)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_falls_back_to_none(self):
        with self.assertLogs(common.LOGGER, level="WARNING"):
            result = common.resolve_default_id_field(self.shp_path, None, None)
        self.assertIsNone(result)


class TestDropCondition(TestCase):
    def test_small_drop_logs_info(self):
        df = pd.DataFrame({"a": range(1000)})
        with self.assertLogs(common.LOGGER, level="INFO") as logs:
            out = common.drop_condition(df, df.index[:1], "dropping")
        self.assertEqual(len(out), 999)
        self.assertTrue(any(r.levelname == "INFO" for r in logs.records))

    def test_large_drop_warns(self):
        df = pd.DataFrame({"a": range(10)})
        with self.assertLogs(common.LOGGER, level="WARNING"):
            out = common.drop_condition(df, df.index[:5], "dropping")
        self.assertEqual(len(out), 5)


class TestWritePartitionGuards(TestCase):
    INDEXER = H3VectorIndexer(dggs="h3")

    def test_empty_frame_writes_nothing(self):
        with tempfile.TemporaryDirectory() as d:
            n = common.write_partition(
                pd.DataFrame(),
                None,
                Path(d),
                "p",
                "c",
                "snappy",
                self.INDEXER,
                "string",
                False,
            )
            self.assertEqual(n, 0)
            self.assertFalse(list(Path(d).iterdir()))

    def test_missing_partition_column_raises(self):
        df = pd.DataFrame({"c": ["a"], "x": [1]}).set_index("c")
        with tempfile.TemporaryDirectory() as d, self.assertRaises(KeyError):
            common.write_partition(
                df, None, Path(d), "p", "c", "snappy", self.INDEXER, "string", False
            )

    def test_all_null_cell_ids_write_nothing(self):
        df = pd.DataFrame({"p": ["x"], "c": [None]})
        with tempfile.TemporaryDirectory() as d:
            n = common.write_partition(
                df, None, Path(d), "p", "c", "snappy", self.INDEXER, "string", False
            )
            self.assertEqual(n, 0)
            self.assertFalse(list(Path(d).iterdir()))

    def test_mutates_input_in_place(self):
        # See issue #182: write_partition no longer defensively copies its
        # input (the caller, _polyfill(), computes what it needs from the
        # original before calling this). Locking in the "may mutate"
        # contract here so a defensive copy doesn't silently creep back in.
        # H3 is int-native (#199): placeholder ints, not real cells - this
        # only checks the generic mutation contract, not H3 correctness.
        df = pd.DataFrame({"c": [10, 20], "p": [1, 2]})
        with tempfile.TemporaryDirectory() as d:
            common.write_partition(
                df, None, Path(d), "p", "c", "snappy", self.INDEXER, "string", False
            )
        self.assertEqual(df["p"].dtype, "string")


class TestStagedFileChunks(TestCase):
    """
    _staged_file_chunks bounds staged files by estimated cell output, not
    just row count (see issue #179: row-count-only sizing let a run of
    similarly huge bisected features concentrate enough cells in one worker
    task to exhaust memory).

    A fake dggs with a 1 m^2 "cell" is registered so bbox area (in an
    unset/planar CRS, i.e. no degree conversion) maps 1:1 to estimated cell
    count, making test geometries exact rather than approximate.
    """

    def setUp(self):
        self.area_patch = mock.patch.dict(
            const.DGGS_CELL_AREA_M2_BY_RES, {"testdggs": lambda res: 1.0}
        )
        self.area_patch.start()
        self.addCleanup(self.area_patch.stop)

    def _chunks(self, sizes, max_rows, budget):
        # sizes[i] is the desired estimated-cell count (== bbox area, m^2)
        # of row i
        geoms = [box(0, 0, math.sqrt(s), math.sqrt(s)) for s in sizes]
        df = gpd.GeoDataFrame({"geometry": geoms})
        with mock.patch.object(const, "MAX_CELLS_PER_STAGED_FILE", budget):
            return list(common._staged_file_chunks(df, "testdggs", 1, max_rows))

    def test_small_uniform_batch_stays_in_one_chunk(self):
        chunks = self._chunks([1] * 1000, max_rows=2000, budget=500_000)
        self.assertEqual(chunks, [(0, 1000)])

    def test_row_count_backstop_still_applies_under_budget(self):
        # cell budget is nowhere near reached; only the row-count cap should
        # split this into two files
        chunks = self._chunks([1] * 1000, max_rows=500, budget=500_000)
        self.assertEqual(chunks, [(0, 500), (500, 1000)])

    def test_large_rows_split_by_cell_budget_despite_low_row_count(self):
        # three rows of 300 "cells" each; budget of 500 means no two can
        # share a file, even though max_rows would allow it
        chunks = self._chunks([300, 300, 300], max_rows=100, budget=500)
        self.assertEqual(chunks, [(0, 1), (1, 2), (2, 3)])

    def test_single_oversized_row_still_gets_its_own_chunk(self):
        # a single row far exceeding the budget must not be dropped or loop
        # forever -- it gets a (degenerate) chunk of its own
        chunks = self._chunks([10_000], max_rows=100, budget=500)
        self.assertEqual(chunks, [(0, 1)])

    def test_empty_batch_yields_no_chunks(self):
        df = gpd.GeoDataFrame({"geometry": []})
        chunks = list(common._staged_file_chunks(df, "testdggs", 1, 100))
        self.assertEqual(chunks, [])

    def test_geographic_crs_converts_degrees_before_estimating(self):
        # ~1 degree square at the equator is ~(111km)^2, not ~1 m^2. With
        # the conversion, two such rows blow a budget of 1000 "cells" and
        # must split; without it, raw degree^2 area (~1 each) would stay
        # well under budget and wrongly stay in one file.
        df = gpd.GeoDataFrame(
            {"geometry": [box(0, 0, 1, 1), box(2, 0, 3, 1)]}, crs="EPSG:4326"
        )
        with mock.patch.object(const, "MAX_CELLS_PER_STAGED_FILE", 1000):
            chunks = list(common._staged_file_chunks(df, "testdggs", 1, 100))
        self.assertEqual(chunks, [(0, 1), (1, 2)])


class TestDictionaryEncodeAttributes(TestCase):
    """
    _dictionary_encode_attributes casts string attribute columns to category
    dtype (see issue #181: --keep_attributes duplicates the full attribute
    payload onto every generated cell; dictionary encoding turns repeated
    values into small integer codes referencing one shared dictionary,
    rather than independent string copies per cell).
    """

    def test_object_columns_become_categorical(self):
        gdf = gpd.GeoDataFrame(
            {
                "geometry": [Point(0, 0), Point(1, 1)],
                "class": ["forest", "wetland"],
                "count": [1, 2],
            }
        )
        result = common._dictionary_encode_attributes(gdf)
        self.assertEqual(result["class"].dtype, "category")
        self.assertEqual(list(result["class"]), ["forest", "wetland"])

    def test_numeric_and_geometry_columns_untouched(self):
        gdf = gpd.GeoDataFrame({"geometry": [Point(0, 0)], "count": [1]})
        result = common._dictionary_encode_attributes(gdf)
        self.assertEqual(result["count"].dtype, gdf["count"].dtype)
        self.assertEqual(result.geometry.name, "geometry")

    def test_no_string_columns_is_a_no_op(self):
        gdf = gpd.GeoDataFrame({"geometry": [Point(0, 0)], "count": [1]})
        result = common._dictionary_encode_attributes(gdf)
        pd.testing.assert_frame_equal(result, gdf)

    def test_prepare_dataframe_encodes_only_when_keeping_attributes(self):
        gdf = gpd.GeoDataFrame({"geometry": [Point(0, 0)], "class": ["forest"]})

        kept = common._prepare_dataframe(gdf.copy(), None, keep_attributes=True)
        self.assertEqual(kept["class"].dtype, "category")

        dropped = common._prepare_dataframe(gdf.copy(), None, keep_attributes=False)
        self.assertNotIn("class", dropped.columns)


class TestCommitOutput(TestCase):
    def test_refuses_directory_created_mid_run_without_overwrite(self):
        with tempfile.TemporaryDirectory() as d:
            staging = Path(d) / ".out.staging"
            staging.mkdir()
            (staging / "new.parquet").touch()
            target = Path(d) / "out"
            target.mkdir()  # appeared after validation, mid-run
            (target / "theirs.txt").touch()
            with self.assertRaises(FileExistsError):
                common._commit_output(staging, target, overwrite=False)
            self.assertTrue((target / "theirs.txt").exists())

    def test_replaces_existing_with_overwrite(self):
        with tempfile.TemporaryDirectory() as d:
            staging = Path(d) / ".out.staging"
            staging.mkdir()
            (staging / "new.parquet").touch()
            target = Path(d) / "out"
            target.mkdir()
            (target / "old.parquet").touch()
            result = common._commit_output(staging, target, overwrite=True)
            self.assertEqual([p.name for p in Path(result).iterdir()], ["new.parquet"])
            self.assertFalse(staging.exists())


class TestMergePartitionFilesDtypeSafety(TestCase):
    """
    _merge_partition_files's schema-unification step normalises string vs
    large_string mismatches across part files, but left an equivalent gap
    for numeric cell-id dtype mismatches: pyarrow's own "permissive" schema
    unification silently picks int64 over uint64 for a mismatched field,
    which would corrupt a genuine uint64 cell ID above 2**63-1 on cast.
    Reproduced here with synthetic part files, ahead of any real backend
    adopting a native int cell-id form.
    """

    def test_int64_uint64_mismatch_unifies_to_uint64_not_silently_narrowed(self):
        with tempfile.TemporaryDirectory() as d:
            partition_dir = Path(d)
            pq.write_table(
                pa.table({"cell": pa.array([1, 2, 3], type=pa.uint64())}),
                partition_dir / "part-0.parquet",
            )
            pq.write_table(
                pa.table({"cell": pa.array([4, 5, 6], type=pa.int64())}),
                partition_dir / "part-1.parquet",
            )

            common._merge_partition_files(
                partition_dir, compression="snappy", dggs_col="cell", id_col="fid"
            )

            merged = list(partition_dir.glob("*.parquet"))
            self.assertEqual(len(merged), 1)
            table = pq.read_table(merged[0])
            self.assertEqual(table.schema.field("cell").type, pa.uint64())
            self.assertEqual(
                sorted(table.column("cell").to_pylist()), [1, 2, 3, 4, 5, 6]
            )


def _pairs_table(fids, cells, extra=None):
    data = {"fid": fids, "h3_09": cells}
    if extra is not None:
        data["attr"] = extra
    return pa.table(data)


class TestDropDuplicateCells(TestCase):
    """
    One row per (feature, cell). A feature split into several geometries
    upstream fills each one independently, and any containment test on a
    cell's area - unlike a centre-point test, where a cell centre falls in
    exactly one piece - can emit the same cell from more than one of them.
    """

    def test_exact_duplicates_collapse_to_one(self):
        table = _pairs_table([1, 1, 2], ["a", "a", "b"], extra=["x", "x", "y"])
        result = common._drop_duplicate_cells(table, "fid", "h3_09")
        self.assertEqual(result.num_rows, 2)
        self.assertEqual(result.column("fid").to_pylist(), [1, 2])
        self.assertEqual(result.column("h3_09").to_pylist(), ["a", "b"])
        self.assertEqual(result.column("attr").to_pylist(), ["x", "y"])

    def test_same_cell_for_different_features_is_kept(self):
        # overlapping features legitimately share cells; only the pair is
        # what has to be unique
        table = _pairs_table([1, 2], ["a", "a"])
        result = common._drop_duplicate_cells(table, "fid", "h3_09")
        self.assertEqual(result.num_rows, 2)

    def test_same_feature_with_different_cells_is_kept(self):
        table = _pairs_table([1, 1], ["a", "b"])
        result = common._drop_duplicate_cells(table, "fid", "h3_09")
        self.assertEqual(result.num_rows, 2)

    def test_row_order_is_preserved(self):
        # write_partition sorts by partition value so each parent cell's
        # rows stay contiguous; deduplication must not reshuffle that
        table = _pairs_table([3, 1, 3, 2, 1], ["c", "a", "c", "b", "a"])
        result = common._drop_duplicate_cells(table, "fid", "h3_09")
        self.assertEqual(result.column("h3_09").to_pylist(), ["c", "a", "b"])

    def test_unduplicated_table_is_returned_unchanged(self):
        table = _pairs_table([1, 2, 3], ["a", "b", "c"])
        self.assertIs(common._drop_duplicate_cells(table, "fid", "h3_09"), table)

    def test_schema_metadata_survives(self):
        table = _pairs_table([1, 1], ["a", "a"]).replace_schema_metadata(
            {b"geo": b"{}"}
        )
        result = common._drop_duplicate_cells(table, "fid", "h3_09")
        self.assertEqual(result.num_rows, 1)
        self.assertEqual(result.schema.metadata.get(b"geo"), b"{}")

    def test_missing_key_column_is_a_no_op(self):
        table = pa.table({"h3_09": ["a", "a"]})
        self.assertIs(common._drop_duplicate_cells(table, "fid", "h3_09"), table)


class TestMergePartitionFilesDeduplication(TestCase):
    """
    The merge step is where deduplication can be both complete and cheap: a
    cell and every duplicate of it share a parent, so the hive write has
    already gathered them into one directory.
    """

    def _write(self, partition_dir, name, fids, cells):
        pq.write_table(_pairs_table(fids, cells), partition_dir / name)

    def test_duplicates_within_a_single_file_are_dropped(self):
        # a feature's pieces can land in one staged file, so a lone part
        # file is not evidence that there is nothing to drop
        with tempfile.TemporaryDirectory() as d:
            partition_dir = Path(d)
            self._write(partition_dir, "part-0.parquet", [1, 1, 2], ["a", "a", "b"])
            dropped, ids = common._merge_partition_files(
                partition_dir, compression="snappy", dggs_col="h3_09", id_col="fid"
            )
            self.assertEqual(dropped, 1)
            self.assertEqual(ids, {1, 2})
            merged = list(partition_dir.glob("*.parquet"))
            self.assertEqual(len(merged), 1)
            self.assertEqual(pq.read_table(merged[0]).num_rows, 2)

    def test_duplicates_across_files_are_dropped(self):
        # pieces of one feature can also be split across staged files, which
        # is why this cannot be done before the merge
        with tempfile.TemporaryDirectory() as d:
            partition_dir = Path(d)
            self._write(partition_dir, "part-0.parquet", [1, 2], ["a", "b"])
            self._write(partition_dir, "part-1.parquet", [1, 3], ["a", "c"])
            dropped, ids = common._merge_partition_files(
                partition_dir, compression="snappy", dggs_col="h3_09", id_col="fid"
            )
            self.assertEqual(dropped, 1)
            self.assertEqual(ids, {1, 2, 3})
            merged = list(partition_dir.glob("*.parquet"))
            self.assertEqual(len(merged), 1)
            table = pq.read_table(merged[0])
            self.assertEqual(table.num_rows, 3)
            self.assertEqual(
                sorted(
                    zip(
                        table.column("fid").to_pylist(),
                        table.column("h3_09").to_pylist(),
                        strict=True,
                    )
                ),
                [(1, "a"), (2, "b"), (3, "c")],
            )

    def test_clean_single_file_is_left_untouched(self):
        # the pre-existing no-work shortcut: one file, nothing to drop, so
        # no rewrite (and the file keeps its name)
        with tempfile.TemporaryDirectory() as d:
            partition_dir = Path(d)
            self._write(partition_dir, "part-0.parquet", [1, 2], ["a", "b"])
            before = {f.name for f in partition_dir.glob("*.parquet")}
            dropped, ids = common._merge_partition_files(
                partition_dir, compression="snappy", dggs_col="h3_09", id_col="fid"
            )
            self.assertEqual(dropped, 0)
            # reported even though the file was left alone: the ids present
            # in the output are what the dropped-feature count is taken from
            self.assertEqual(ids, {1, 2})
            self.assertEqual({f.name for f in partition_dir.glob("*.parquet")}, before)
