import pyarrow as pa
import pyarrow.parquet as pq

from vector2dggs.a5 import a5
from vector2dggs.h3 import h3

from ..data.datapaths import TEST_FILE_PATH, TEST_LAYER_NAME
from .base import TestRunthrough, skip_unless_backend


class TestA5CellIdUint64(TestRunthrough):
    """
    End-to-end regression for issue #198: --cell-id uint64 output must be a
    genuine uint64 Arrow column, never float64 (pandas silently promotes
    uint64 to float64 wherever it meets int64/float, corrupting cell IDs)
    or int64 (pyarrow's own schema-unification can silently narrow to it
    across mismatched part files). A small -c forces multiple staged files
    per hive partition (exercising the merge path), and -co forces
    compaction (exercising compaction_common) - together, the highest-risk
    combination identified while designing #197/#198.

    These check only the fine cell column (dggs_col, e.g. a5_17): the
    parent/partition column (e.g. a5_11) is a separate, pre-existing
    concern (issue #202 - it leaks into row data under some conditions,
    reproduced identically on plain main with today's default string
    output, so it's out of scope here).
    """

    DGGS_COL = "a5_17"

    @classmethod
    def setUpClass(cls):
        skip_unless_backend("a5")
        super().setUpClass()

    def _run(self, extra=()):
        a5(
            [
                TEST_FILE_PATH,
                str(self.output_path),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "17",
                "-id",
                "LCDB_UID",
                "-t",
                "1",
                *extra,
            ],
            standalone_mode=False,
        )

    def _dggs_col_type(self):
        files = sorted(self.output_path.rglob("*.parquet"))
        self.assertTrue(files, "no parquet output written")
        table = pq.read_table(files[0])
        self.assertIn(self.DGGS_COL, table.schema.names)
        return table, table.schema.field(self.DGGS_COL).type

    def test_uint64_output_no_compaction(self):
        self._run(("--cell-id", "uint64"))
        _, field_type = self._dggs_col_type()
        self.assertEqual(field_type, pa.uint64())

    def test_uint64_output_with_compaction_and_scattered_partitions(self):
        # a small cut_threshold scatters each feature's cells across many
        # staged files, forcing _merge_partition_files to actually merge
        # (not just pass through a single file) before compacting.
        self._run(("--cell-id", "uint64", "-co", "-c", "50000"))
        _, field_type = self._dggs_col_type()
        self.assertEqual(field_type, pa.uint64())

    def test_string_output_unaffected(self):
        """Default (string) output is unchanged by #198: still a string
        column, values still valid a5 hex tokens - not a behaviour change
        for existing users."""
        import a5 as a5py

        self._run(())
        table, field_type = self._dggs_col_type()
        self.assertIn(field_type, (pa.string(), pa.large_utf8()))
        df = table.to_pandas()
        tokens = df[self.DGGS_COL] if self.DGGS_COL in df.columns else df.index
        for token in list(tokens)[:20]:
            # raises if not a valid a5 hex token
            a5py.hex_to_u64(token)


class TestH3CellIdUint64(TestRunthrough):
    """
    End-to-end regression for issue #199, mirroring TestA5CellIdUint64
    (see its docstring for the dtype-safety rationale).
    """

    DGGS_COL = "h3_09"

    @classmethod
    def setUpClass(cls):
        skip_unless_backend("h3")
        super().setUpClass()

    def _run(self, extra=()):
        h3(
            [
                TEST_FILE_PATH,
                str(self.output_path),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "9",
                "-id",
                "LCDB_UID",
                "-t",
                "1",
                *extra,
            ],
            standalone_mode=False,
        )

    def _dggs_col_type(self):
        files = sorted(self.output_path.rglob("*.parquet"))
        self.assertTrue(files, "no parquet output written")
        table = pq.read_table(files[0])
        self.assertIn(self.DGGS_COL, table.schema.names)
        return table, table.schema.field(self.DGGS_COL).type

    def test_uint64_output_no_compaction(self):
        self._run(("--cell-id", "uint64"))
        _, field_type = self._dggs_col_type()
        self.assertEqual(field_type, pa.uint64())

    def test_uint64_output_with_compaction_and_scattered_partitions(self):
        self._run(("--cell-id", "uint64", "-co", "-c", "50000"))
        _, field_type = self._dggs_col_type()
        self.assertEqual(field_type, pa.uint64())

    def test_string_output_unaffected(self):
        """Default (string) output is unchanged by #199: still a string
        column, values still valid h3 tokens - not a behaviour change for
        existing users."""
        import h3 as h3py

        self._run(())
        table, field_type = self._dggs_col_type()
        self.assertIn(field_type, (pa.string(), pa.large_utf8()))
        df = table.to_pandas()
        tokens = df[self.DGGS_COL] if self.DGGS_COL in df.columns else df.index
        for token in list(tokens)[:20]:
            # raises if not a valid h3 string token
            h3py.str_to_int(token)
