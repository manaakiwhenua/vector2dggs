import warnings
from collections import Counter
from unittest import TestCase

import geopandas as gpd
import h3 as h3lib
import pandas as pd
from shapely.geometry import box

from vector2dggs.h3 import h3
from vector2dggs.indexers.h3vectorindexer import H3VectorIndexer

from ..data.datapaths import TEST_FILE_PATH, TEST_LAYER_NAME
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
        # small -c and -ch scatter each feature across many partitions
        from vector2dggs import common

        common.index(
            "h3",
            TEST_FILE_PATH,
            str(self.output_path),
            11,
            8,
            False,
            1,
            cut_threshold=50000.0,
            id_field="LCDB_UID",
            layer=TEST_LAYER_NAME,
            compact=True,
        )

        df = pd.read_parquet(self.output_path)
        cells_by_feature: dict[str, set[str]] = {}
        for cell, feature_id in set(zip(df.index, df["LCDB_UID"], strict=True)):
            cells_by_feature.setdefault(feature_id, set()).add(cell)

        # all 7 children present for one feature => should have been compacted
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


class TestCompositeIdCompaction(TestRunthrough):
    """
    Compaction groups on the joint key of a composite -id (#194): three
    polygons keyed (1,x), (1,y), (2,x), the two a=1 ones overlapping, so
    neither column is unique alone. Merging siblings on `a` alone would
    surface cells of (1,x) under (1,y) and the per-key expansion below would
    no longer reproduce the uncompacted output.
    """

    RES, PARENT_RES = 9, 5

    def _run(self, src, out, extra=()):
        h3(
            [str(src), str(out), "-r", str(self.RES), "-pr", str(self.PARENT_RES)]
            + ["-id", "a", "-id", "b", "-t", "1", *extra],
            standalone_mode=False,
        )
        return pd.read_parquet(out)

    @staticmethod
    def _cells_by_key(df, expand_to=None):
        out: dict[tuple, set[str]] = {}
        for cell, a, b in zip(df.index, df["a"], df["b"], strict=True):
            cells = (
                h3lib.cell_to_children(cell, expand_to)
                if expand_to is not None and h3lib.get_resolution(cell) < expand_to
                else [cell]
            )
            out.setdefault((a, b), set()).update(cells)
        return out

    def test_compaction_groups_on_joint_key(self):
        src = self.output_path.parent / "composite.gpkg"
        gdf = gpd.GeoDataFrame(
            {
                "a": [1, 1, 2],
                "b": ["x", "y", "x"],
                "geometry": [
                    box(174.70, -41.30, 174.74, -41.26),
                    box(174.72, -41.28, 174.76, -41.24),  # overlaps the first
                    box(174.80, -41.30, 174.84, -41.26),
                ],
            },
            crs=4326,
        )
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            gdf.to_file(src, layer="x")

        flat = self._run(src, self.output_path / "flat")
        compact = self._run(src, self.output_path / "compact", ("-co",))

        self.assertEqual(list(compact.columns), ["a", "b", "h3_05"])
        self.assertLess(len(compact), len(flat))
        self.assertEqual(len(compact[["a", "b"]].drop_duplicates()), 3)
        self.assertEqual(
            self._cells_by_key(compact, expand_to=self.RES), self._cells_by_key(flat)
        )
