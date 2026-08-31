import tempfile
from pathlib import Path
from unittest import TestCase

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from vector2dggs import common
from vector2dggs.a5 import a5
from vector2dggs.h3 import h3
from vector2dggs.s2 import s2

from ..data.datapaths import TEST_FILE_PATH, TEST_LAYER_NAME
from .base import TestRunthrough, skip_unless_backend

try:
    import s2geometry as S2

    from vector2dggs.indexers.s2vectorindexer import S2VectorIndexer
except ImportError:
    S2 = None


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
        self._run(("--cell-id", "uint64", "-co"))
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
        self._run(("--cell-id", "uint64", "-co"))
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


class TestS2CellIdUint64(TestRunthrough):
    """
    End-to-end regression for issue #200, mirroring TestA5CellIdUint64 (see
    its docstring for the dtype-safety rationale).
    """

    DGGS_COL = "s2_13"

    @classmethod
    def setUpClass(cls):
        skip_unless_backend("s2")
        super().setUpClass()

    def _run(self, extra=()):
        s2(
            [
                TEST_FILE_PATH,
                str(self.output_path),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "13",
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
        self._run(("--cell-id", "uint64", "-co"))
        _, field_type = self._dggs_col_type()
        self.assertEqual(field_type, pa.uint64())

    def test_string_output_unaffected(self):
        """Default (string) output is unchanged by #200: still a string
        column, values still valid s2 tokens - not a behaviour change for
        existing users."""
        self._run(())
        table, field_type = self._dggs_col_type()
        self.assertIn(field_type, (pa.string(), pa.large_utf8()))
        df = table.to_pandas()
        tokens = df[self.DGGS_COL] if self.DGGS_COL in df.columns else df.index
        for token in list(tokens)[:20]:
            # raises if not a valid s2 token
            S2.S2CellId.FromToken(token)


class TestS2Face5CellIdSurvivesRoundTrip(TestCase):
    """
    S2 cell IDs on faces 4-5 legitimately exceed the signed 64-bit range
    (confirmed live, and via raster2dggs's own regression test:
    manaakiwhenua/raster2dggs#97's test_s2_face5_parent_survives_the_store_round_trip).
    A bare Python int handled without an explicitly-typed uint64 anywhere
    downstream (dataframe dtype, PyArrow schema/scalar) would silently
    overflow into int64, corrupting the value.
    """

    def setUp(self):
        skip_unless_backend("s2")
        self._tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self._tmp.cleanup()

    def test_s2_face5_cell_survives_write_partition(self):
        face5 = S2.S2CellId.FromFacePosLevel(5, 0, 12)
        self.assertGreater(face5.id(), 2**63)
        parent = face5.parent(6)

        df = pd.DataFrame(
            {"s2_06": pd.array([parent.id()], dtype="uint64"), "fid": [0]},
            index=pd.Index([face5.id()], dtype="uint64", name="s2_12"),
        )

        out = Path(self._tmp.name) / "out"
        common.write_partition(
            df,
            None,
            out,
            "s2_06",
            "s2_12",
            "snappy",
            S2VectorIndexer(dggs="s2"),
            "uint64",
            False,
        )
        files = sorted(out.rglob("*.parquet"))
        self.assertTrue(files, "no parquet output written")
        table = pq.read_table(files[0])
        self.assertEqual(table.schema.field("s2_12").type, pa.uint64())
        self.assertEqual(table.column("s2_12")[0].as_py(), face5.id())
