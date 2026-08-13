import tempfile
from pathlib import Path
from unittest import TestCase, mock, skipIf

import h3 as h3lib
import pandas as pd

from vector2dggs import common
from vector2dggs.indexers.h3vectorindexer import H3VectorIndexer


@skipIf(common.resource is None, "resource module is POSIX-only")
class TestRaiseRlimitNofile(TestCase):
    def test_soft_limit_raised_to_hard(self):
        res = common.resource
        original = res.getrlimit(res.RLIMIT_NOFILE)
        try:
            common.raise_rlimit_nofile()
            soft, hard = res.getrlimit(res.RLIMIT_NOFILE)
            self.assertEqual(soft, hard)
        finally:
            res.setrlimit(res.RLIMIT_NOFILE, original)


class TestSortedHiveWrite(TestCase):
    """
    With rows interleaved across more parent cells than the open-file pool
    holds, the write must still produce one file per parent-cell directory
    (rows sorted by partition column, so no writer churn).
    """

    def test_one_file_per_partition_dir_under_small_budget(self):
        parents = [
            h3lib.latlng_to_cell(-41.0 - 0.1 * i, 174.0 + 0.1 * i, 8) for i in range(30)
        ]
        # Interleaved round-robin across parents, large enough to span many
        # write batches: worst case for an LRU writer pool.
        rows = []
        for _ in range(700):
            for k in range(7):
                for p in parents:
                    child = h3lib.cell_to_center_child(p, 11)
                    rows.append((h3lib.cell_to_children(child, 12)[k], p))
        df = pd.DataFrame(
            {"h3_08": [p for _, p in rows], "fid": range(len(rows))},
            index=pd.Index([c for c, _ in rows], name="h3_12"),
        )

        with tempfile.TemporaryDirectory() as out, mock.patch.object(
            common.const, "MAX_OPEN_FILES_PER_TASK", 8
        ):
            common.write_partition_as_geoparquet(
                df,
                H3VectorIndexer.cell_to_polygon,
                Path(out),
                "h3_08",
                "h3_12",
                "snappy",
            )
            counts = [
                len(list(d.glob("*.parquet")))
                for d in Path(out).iterdir()
                if d.is_dir()
            ]
            self.assertEqual(len(counts), 30)
            self.assertEqual(max(counts), 1, f"fragmented dirs: {sorted(counts)}")
