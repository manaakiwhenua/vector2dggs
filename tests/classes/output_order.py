"""
Output rows are sorted by cell ID within every file, and the cell ID column is
delta-encoded, for both ID forms, both output kinds, and with compaction.
"""

import pyarrow.parquet as pq

from vector2dggs.h3 import h3

from ..data.datapaths import TEST_FILE_PATH, TEST_LAYER_NAME
from .base import TestRunthrough, skip_unless_backend

DGGS_COL = "h3_10"


def _column_encodings(path):
    meta = pq.ParquetFile(path).metadata.row_group(0)
    return {
        meta.column(i).path_in_schema: meta.column(i).encodings
        for i in range(meta.num_columns)
    }


class TestOutputOrder(TestRunthrough):
    @classmethod
    def setUpClass(cls):
        skip_unless_backend("h3")
        super().setUpClass()

    def _run(self, *extra):
        h3(
            [
                TEST_FILE_PATH,
                str(self.output_path),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "10",
                "-id",
                "LCDB_UID",
                "-t",
                "1",
                *extra,
            ],
            standalone_mode=False,
        )
        files = sorted(self.output_path.rglob("*.parquet"))
        self.assertTrue(files, "no parquet output written")
        return files

    def _check(self, files, encoding):
        for f in files:
            cells = pq.read_table(f, columns=[DGGS_COL]).column(DGGS_COL).to_pylist()
            self.assertEqual(cells, sorted(cells), f"{f} is not in cell ID order")
            encodings = _column_encodings(f)
            self.assertIn(encoding, encodings[DGGS_COL], encodings)
            # the other columns keep the default dictionary encoding
            self.assertIn("RLE_DICTIONARY", encodings["LCDB_UID"], encodings)

    def test_string_ids(self):
        self._check(self._run(), "DELTA_BYTE_ARRAY")

    def test_string_ids_compacted(self):
        self._check(self._run("-co"), "DELTA_BYTE_ARRAY")

    def test_uint64_ids(self):
        self._check(self._run("--cell-id", "uint64"), "DELTA_BINARY_PACKED")

    def test_uint64_ids_compacted_geoparquet(self):
        self._check(
            self._run("--cell-id", "uint64", "-co", "--geo", "polygon"),
            "DELTA_BINARY_PACKED",
        )

    def test_single_file_partition_is_rewritten(self):
        # one staged file per partition used to be left as written; it must
        # still come out sorted and delta-encoded
        self._check(self._run("-pr", "4"), "DELTA_BYTE_ARRAY")
