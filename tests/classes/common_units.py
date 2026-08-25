import tempfile
from pathlib import Path
from unittest import TestCase, mock

import pandas as pd

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
