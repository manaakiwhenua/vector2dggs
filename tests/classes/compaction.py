from itertools import product
from unittest import TestCase

import numpy as np
import pandas as pd

from vector2dggs.indexers.vectorindexer import VectorIndexer

from .base import skip_unless_backend

try:
    import h3

    from vector2dggs.indexers.h3vectorindexer import H3VectorIndexer
except ImportError:
    h3 = None
try:
    import a5

    from vector2dggs.indexers.a5vectorindexer import A5VectorIndexer
except ImportError:
    a5 = None
try:
    import s2geometry as S2

    from vector2dggs.indexers.s2vectorindexer import S2VectorIndexer
except ImportError:
    S2 = None
try:
    from rhealpixdggs.rhp_wrappers import rhp_get_resolution

    from vector2dggs.indexers.rhpvectorindexer import RHPVectorIndexer
except ImportError:
    rhp_get_resolution = None
try:
    from vector2dggs.indexers.geohashvectorindexer import GeohashVectorIndexer
except ImportError:
    GeohashVectorIndexer = None


class TestH3CompactionBounds(TestCase):
    """
    Compaction must never produce cells coarser (lower resolution) than
    parent_res, even when a feature's cells fully cover an ancestor that is
    coarser than parent_res.
    """

    def setUp(self):
        skip_unless_backend("h3")
        self.parent_res = 5
        # An ancestor one level coarser than parent_res, fully covered by all
        # of its grandchildren at parent_res + 1.
        self.ancestor = h3.latlng_to_cell(-41.0, 174.0, self.parent_res - 1)
        self.res = self.parent_res + 1
        self.cells = set(h3.cell_to_children(self.ancestor, self.res))

    def test_unbounded_compaction_would_exceed_parent_res(self):
        # Demonstrates the underlying behaviour that necessitates the floor:
        # h3.compact_cells has no resolution floor and will compact past
        # parent_res whenever the cells fully cover a coarser ancestor.
        unbounded = set(h3.compact_cells(self.cells))
        self.assertEqual(unbounded, {self.ancestor})
        self.assertLess(h3.get_resolution(self.ancestor), self.parent_res)

    def test_enforce_resolution_floor_breaks_up_coarse_cells(self):
        result = VectorIndexer._enforce_resolution_floor(
            {self.ancestor}, self.parent_res, h3.get_resolution, h3.cell_to_children
        )

        self.assertTrue(all(h3.get_resolution(c) >= self.parent_res for c in result))
        self.assertEqual(
            result, set(h3.cell_to_children(self.ancestor, self.parent_res))
        )

    def test_compaction_respects_parent_res(self):
        indexer = H3VectorIndexer(dggs="h3")
        dggs_col = f"h3_{self.res:02}"
        df = pd.DataFrame(
            {
                "id": [1] * len(self.cells),
                "attr": range(len(self.cells)),
                dggs_col: list(self.cells),
            }
        )

        result = indexer.compaction(
            df, self.res, ["id", "attr"], dggs_col, "id", self.parent_res
        )

        self.assertTrue(
            all(h3.get_resolution(c) >= self.parent_res for c in result.index)
        )


class TestGeohashCompactionBounds(TestCase):
    """
    Compaction must never produce cells shorter than parent_res, even when a
    feature's cells fully cover an ancestor that is shorter than parent_res.
    """

    def setUp(self):
        skip_unless_backend("geohash")
        self.parent_res = 2
        self.ancestor = "s"
        self.res = self.parent_res + 1
        self.indexer = GeohashVectorIndexer(dggs="geohash")
        chars = sorted(self.indexer.GEOHASH_BASE32_SET)
        self.cells = {self.ancestor + a + b for a, b in product(chars, repeat=2)}

    def test_unbounded_compaction_would_exceed_parent_res(self):
        # Demonstrates the underlying behaviour that necessitates the floor:
        # compact() has no length floor and will compact past parent_res
        # whenever the cells fully cover a shorter ancestor.
        unbounded = self.indexer.compact(self.cells)
        self.assertEqual(unbounded, {self.ancestor})
        self.assertLess(len(self.ancestor), self.parent_res)

    def test_children_at_res(self):
        chars = sorted(self.indexer.GEOHASH_BASE32_SET)
        result = self.indexer.children_at_res(self.ancestor, self.parent_res)

        self.assertEqual(set(result), {self.ancestor + a for a in chars})

    def test_enforce_resolution_floor_breaks_up_coarse_cells(self):
        result = VectorIndexer._enforce_resolution_floor(
            {self.ancestor}, self.parent_res, len, self.indexer.children_at_res
        )

        chars = sorted(self.indexer.GEOHASH_BASE32_SET)
        self.assertTrue(all(len(c) >= self.parent_res for c in result))
        self.assertEqual(result, {self.ancestor + a for a in chars})

    def test_compaction_respects_parent_res(self):
        dggs_col = f"geohash_{self.res:02}"
        df = pd.DataFrame(
            {
                "id": [1] * len(self.cells),
                "attr": range(len(self.cells)),
                dggs_col: list(self.cells),
            }
        )

        result = self.indexer.compaction(
            df, self.res, ["id", "attr"], dggs_col, "id", self.parent_res
        )

        self.assertTrue(all(len(c) >= self.parent_res for c in result.index))


class TestRHPCompactionBounds(TestCase):
    """
    Compaction must never produce cells coarser (lower resolution) than
    parent_res, even when a feature's cells fully cover an ancestor that is
    coarser than parent_res.
    """

    def setUp(self):
        skip_unless_backend("rhp")
        self.parent_res = 5
        # An ancestor one level coarser than parent_res, fully covered by all
        # of its grandchildren at parent_res + 1.
        self.ancestor = "N0000"
        self.res = self.parent_res + 1
        self.indexer = RHPVectorIndexer(dggs="rhp")
        digits = "012345678"
        self.cells = {self.ancestor + a + b for a, b in product(digits, repeat=2)}

    def test_unbounded_compaction_would_exceed_parent_res(self):
        # Demonstrates the underlying behaviour that necessitates the floor:
        # compact_cells has no resolution floor and will compact past
        # parent_res whenever the cells fully cover a coarser ancestor.
        unbounded = self.indexer.compact_cells(self.cells)
        self.assertEqual(unbounded, {self.ancestor})
        self.assertLess(rhp_get_resolution(self.ancestor), self.parent_res)

    def test_children_at_res(self):
        digits = "012345678"
        result = self.indexer.children_at_res(self.ancestor, self.parent_res)

        self.assertEqual(set(result), {self.ancestor + d for d in digits})

    def test_enforce_resolution_floor_breaks_up_coarse_cells(self):
        result = VectorIndexer._enforce_resolution_floor(
            {self.ancestor},
            self.parent_res,
            rhp_get_resolution,
            self.indexer.children_at_res,
        )

        digits = "012345678"
        self.assertTrue(all(rhp_get_resolution(c) >= self.parent_res for c in result))
        self.assertEqual(result, {self.ancestor + d for d in digits})

    def test_compaction_respects_parent_res(self):
        dggs_col = f"rhp_{self.res:02}"
        df = pd.DataFrame(
            {
                "id": [1] * len(self.cells),
                "attr": range(len(self.cells)),
                dggs_col: list(self.cells),
            }
        )

        result = self.indexer.compaction(
            df, self.res, ["id", "attr"], dggs_col, "id", self.parent_res
        )

        self.assertTrue(
            all(rhp_get_resolution(c) >= self.parent_res for c in result.index)
        )


class TestS2CompactionBounds(TestCase):
    """
    Compaction must never produce cells coarser (lower level) than
    parent_res, even when a feature's cells fully cover an ancestor that is
    coarser than parent_res.
    """

    def setUp(self):
        skip_unless_backend("s2")
        self.parent_res = 10
        # An ancestor one level coarser than parent_res, fully covered by all
        # of its grandchildren at parent_res + 1.
        latlng = S2.S2LatLng.FromDegrees(-41.0, 174.0)
        self.ancestor_id = S2.S2CellId(latlng).parent(self.parent_res - 1)
        self.ancestor = self.ancestor_id.ToToken()
        self.res = self.parent_res + 1
        self.indexer = S2VectorIndexer(dggs="s2")
        self.cells = self._descendants(self.ancestor_id, self.res)

    @staticmethod
    def _descendants(cell_id, level):
        begin = cell_id.child_begin(level)
        end = cell_id.child_end(level)
        cells = set()
        cur = begin
        while cur != end:
            cells.add(cur.ToToken())
            cur = cur.next()
        return cells

    @staticmethod
    def _get_resolution(token):
        return S2.S2CellId.FromToken(token).level()

    def test_unbounded_compaction_would_exceed_parent_res(self):
        # Demonstrates the underlying behaviour that necessitates the floor:
        # compact_tokens has no level floor and will compact past parent_res
        # whenever the cells fully cover a coarser ancestor.
        unbounded = self.indexer.compact_tokens(self.cells)
        self.assertEqual(unbounded, {self.ancestor})
        self.assertLess(self._get_resolution(self.ancestor), self.parent_res)

    def test_children_at_res(self):
        result = self.indexer.children_at_res(self.ancestor, self.parent_res)

        self.assertEqual(
            set(result),
            self._descendants(self.ancestor_id, self.parent_res),
        )

    def test_enforce_resolution_floor_breaks_up_coarse_cells(self):
        result = VectorIndexer._enforce_resolution_floor(
            {self.ancestor},
            self.parent_res,
            self._get_resolution,
            self.indexer.children_at_res,
        )

        self.assertTrue(all(self._get_resolution(c) >= self.parent_res for c in result))
        self.assertEqual(result, self._descendants(self.ancestor_id, self.parent_res))

    def test_compaction_respects_parent_res(self):
        dggs_col = f"s2_{self.res:02}"
        df = pd.DataFrame(
            {
                "id": [1] * len(self.cells),
                "attr": range(len(self.cells)),
                dggs_col: list(self.cells),
            }
        )

        result = self.indexer.compaction(
            df, self.res, ["id", "attr"], dggs_col, "id", self.parent_res
        )

        self.assertTrue(
            all(self._get_resolution(c) >= self.parent_res for c in result.index)
        )


class TestA5CompactionBounds(TestCase):
    """
    Compaction must never produce cells coarser (lower resolution) than
    parent_res, even when a feature's cells fully cover an ancestor that is
    coarser than parent_res.
    """

    def setUp(self):
        skip_unless_backend("a5")
        self.parent_res = 2
        # An ancestor one level coarser than parent_res, fully covered by all
        # of its grandchildren at parent_res + 1.
        ancestor_u64 = a5.cell_to_parent(a5.get_res0_cells()[0], self.parent_res - 1)
        self.ancestor = a5.u64_to_hex(ancestor_u64)
        self.res = self.parent_res + 1
        self.indexer = A5VectorIndexer(dggs="a5")
        self.cells = {
            a5.u64_to_hex(c) for c in a5.cell_to_children(ancestor_u64, self.res)
        }

    def test_unbounded_compaction_would_exceed_parent_res(self):
        # Demonstrates the underlying behaviour that necessitates the floor:
        # a5.compact has no resolution floor and will compact past
        # parent_res whenever the cells fully cover a coarser ancestor.
        unbounded = {
            a5.u64_to_hex(c) for c in a5.compact([a5.hex_to_u64(c) for c in self.cells])
        }
        self.assertEqual(unbounded, {self.ancestor})
        self.assertLess(A5VectorIndexer.get_resolution(self.ancestor), self.parent_res)

    def test_children_at_res(self):
        result = self.indexer.children_at_res(self.ancestor, self.parent_res)

        self.assertEqual(
            set(result),
            {
                a5.u64_to_hex(c)
                for c in a5.cell_to_children(
                    a5.hex_to_u64(self.ancestor), self.parent_res
                )
            },
        )

    def test_enforce_resolution_floor_breaks_up_coarse_cells(self):
        result = VectorIndexer._enforce_resolution_floor(
            {self.ancestor},
            self.parent_res,
            A5VectorIndexer.get_resolution,
            self.indexer.children_at_res,
        )

        self.assertTrue(
            all(A5VectorIndexer.get_resolution(c) >= self.parent_res for c in result)
        )
        self.assertEqual(
            set(result),
            set(self.indexer.children_at_res(self.ancestor, self.parent_res)),
        )

    def test_compaction_respects_parent_res(self):
        dggs_col = f"a5_{self.res:02}"
        df = pd.DataFrame(
            {
                "id": [1] * len(self.cells),
                "attr": range(len(self.cells)),
                dggs_col: list(self.cells),
            }
        )

        result = self.indexer.compaction(
            df, self.res, ["id", "attr"], dggs_col, "id", self.parent_res
        )

        self.assertTrue(
            all(
                A5VectorIndexer.get_resolution(c) >= self.parent_res
                for c in result.index
            )
        )


class _SyntheticIndexer(VectorIndexer):
    """A concrete but otherwise-unused VectorIndexer, so compaction_common
    (a real, non-abstract instance method) can be called directly with
    synthetic cell functions - isolating the shared algorithm from any real
    backend's semantics."""

    def _polyfill_polygons(self, df, resolution):
        raise NotImplementedError

    def _polyfill_linestrings(self, df, resolution):
        raise NotImplementedError

    def _polyfill_points(self, df, resolution):
        raise NotImplementedError

    def secondary_index(self, df, parent_res):
        raise NotImplementedError

    def compaction(self, df, res, col_order, dggs_col, id_field, parent_res):
        raise NotImplementedError

    @staticmethod
    def cell_to_point(cell):
        raise NotImplementedError

    @staticmethod
    def cell_to_polygon(cell):
        raise NotImplementedError

    @staticmethod
    def get_resolution(cell):
        raise NotImplementedError

    @staticmethod
    def children_at_res(cell, target_res):
        raise NotImplementedError


class TestCompactionCommonDtypeSafety(TestCase):
    """
    compaction_common builds a fresh pd.Series from a plain Python list of
    compacted cell IDs (parent_for_pair), separately from the incoming
    dataframe's own dggs_col. Left to inference, pandas picks int64 or
    uint64 per list depending on whether any value exceeds 2**63-1,
    independently of the source column's actual dtype - and concatenating a
    frame with a mismatched int64/uint64 index silently upcasts the result
    to float64, corrupting cell IDs. Reproduced here with synthetic
    (non-string) cell IDs, ahead of any real backend adopting them.
    """

    DGGS_COL = "synthetic_02"

    def setUp(self):
        self.indexer = _SyntheticIndexer(dggs="synthetic")
        # Feature A's three fine cells fully cover parent cell 100 and
        # compact away entirely; feature B's one cell has no siblings and
        # stays as-is - so the result concatenates a freshly-built
        # (compacted) frame with a slice of the original (uncompacted) one.
        self.df = pd.DataFrame(
            {
                "id": ["A", "A", "A", "B"],
                "attr": [1, 2, 3, 4],
                self.DGGS_COL: pd.array([1000, 1001, 1002, 2000], dtype="uint64"),
            }
        )

    @staticmethod
    def _compact_func(cells):
        cells = set(cells)
        return {100} if cells == {1000, 1001, 1002} else cells

    @staticmethod
    def _cell_to_child_func(cell, res):
        return {100: 1000}[cell]

    @staticmethod
    def _get_resolution_func(cell):
        return 2 if cell >= 1000 else 1

    @staticmethod
    def _children_at_res_func(cell, target_res):
        return {cell}

    def test_compacted_result_preserves_uint64_dtype(self):
        result = self.indexer.compaction_common(
            self.df,
            res=2,
            id_field="id",
            col_order=["id", "attr"],
            dggs_col=self.DGGS_COL,
            compact_func=self._compact_func,
            cell_to_child_func=self._cell_to_child_func,
            parent_res=1,
            get_resolution_func=self._get_resolution_func,
            children_at_res_func=self._children_at_res_func,
        )

        self.assertEqual(result.index.dtype, np.dtype("uint64"))
        self.assertEqual({int(c) for c in result.index}, {100, 2000})
