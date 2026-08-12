from collections import Counter
from unittest import TestCase

import h3 as h3lib
import pandas as pd

from vector2dggs.h3 import h3
from vector2dggs.indexers.h3vectorindexer import H3VectorIndexer

from ..data.datapaths import TEST_FILE_PATH, TEST_LAYER_NAME, TEST_OUTPUT_PATH
from .base import TestRunthrough


class TestCompactionEmptyPartition(TestCase):
    """
    Compaction receives one dataframe per dask partition, and a partition may
    be empty (e.g. after a shuffle). It must return an empty frame of the
    expected shape rather than raising.
    """

    def test_empty_partition_compacts_to_empty(self):
        df = pd.DataFrame(
            {"h3_11": [], "LCDB_UID": [], "h3_08": []},
        )
        indexer = H3VectorIndexer(dggs="h3")
        result = indexer.compaction(
            df, 11, ["LCDB_UID", "h3_08"], "h3_11", "LCDB_UID", 8
        )
        self.assertEqual(len(result), 0)
        self.assertEqual(result.columns.tolist(), ["LCDB_UID", "h3_08"])
        self.assertEqual(result.index.name, "h3_11")


class TestCompactionAcrossPartitions(TestRunthrough):
    """
    Compaction must produce the same result regardless of how features are
    fragmented across dask partitions: any set of sibling cells belonging to
    one feature that could merge into their parent (down to the parent_res
    floor) must be merged, even when bisection and chunking scatter the
    feature's rows over many partitions.
    """

    def test_compaction_merges_siblings_across_dask_partitions(self):
        # A small cut threshold fragments features into many rows, and a
        # small chunksize spreads those rows across many dask partitions.
        h3(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "11",
                "-pr",
                "8",
                "-id",
                "LCDB_UID",
                "-c",
                "50000",
                "-ch",
                "10",
                "-co",
            ],
            standalone_mode=False,
        )

        df = pd.read_parquet(TEST_OUTPUT_PATH)
        cells_by_feature: dict[str, set[str]] = {}
        for cell, feature_id in set(zip(df.index, df["LCDB_UID"], strict=True)):
            cells_by_feature.setdefault(feature_id, set()).add(cell)

        # A complete set of 7 sibling cells (all children of one parent, at
        # any resolution finer than the parent_res floor) present for a
        # single feature should have been compacted into that parent.
        unmerged_septets = 0
        for cells in cells_by_feature.values():
            parents = Counter(
                h3lib.cell_to_parent(cell, h3lib.get_resolution(cell) - 1)
                for cell in cells
                if h3lib.get_resolution(cell) > 8
            )
            unmerged_septets += sum(1 for n in parents.values() if n == 7)

        self.assertEqual(
            unmerged_septets,
            0,
            f"{unmerged_septets} complete sibling sets were left uncompacted "
            "(feature cells split across dask partitions?)",
        )
