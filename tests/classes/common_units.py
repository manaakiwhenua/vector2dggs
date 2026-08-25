import math
import tempfile
from pathlib import Path
from unittest import TestCase, mock

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, box

import vector2dggs.constants as const
from vector2dggs import common


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
    def test_empty_frame_writes_nothing(self):
        with tempfile.TemporaryDirectory() as d:
            n = common.write_partition(
                pd.DataFrame(), None, Path(d), "p", "c", "snappy"
            )
            self.assertEqual(n, 0)
            self.assertFalse(list(Path(d).iterdir()))

    def test_missing_partition_column_raises(self):
        df = pd.DataFrame({"c": ["a"], "x": [1]}).set_index("c")
        with tempfile.TemporaryDirectory() as d, self.assertRaises(KeyError):
            common.write_partition(df, None, Path(d), "p", "c", "snappy")

    def test_all_null_cell_ids_write_nothing(self):
        df = pd.DataFrame({"p": ["x"], "c": [None]})
        with tempfile.TemporaryDirectory() as d:
            n = common.write_partition(df, None, Path(d), "p", "c", "snappy")
            self.assertEqual(n, 0)
            self.assertFalse(list(Path(d).iterdir()))

    def test_mutates_input_in_place(self):
        # See issue #182: write_partition no longer defensively copies its
        # input (the caller, _polyfill(), computes what it needs from the
        # original before calling this). Locking in the "may mutate"
        # contract here so a defensive copy doesn't silently creep back in.
        df = pd.DataFrame({"c": ["a", "b"], "p": [1, 2]})
        with tempfile.TemporaryDirectory() as d:
            common.write_partition(df, None, Path(d), "p", "c", "snappy")
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
