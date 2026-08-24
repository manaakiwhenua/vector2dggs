import tempfile
from unittest import TestCase, mock

import pandas as pd

import vector2dggs.constants as const
from vector2dggs import common

from ..data.datapaths import TEST_FILE_PATH, TEST_LAYER_NAME


class TestBatchedIngest(TestCase):
    """Reading the input in batches must be invisible: identical (cell, id)
    sets, and synthetic feature ids unique and continuous across batches."""

    def _run(self, out):
        common.index(
            "h3",
            TEST_FILE_PATH,
            out,
            8,
            None,
            False,
            None,
            1,
            layer=TEST_LAYER_NAME,
            compact=False,
        )
        return pd.read_parquet(out)

    def test_batches_equivalent_to_single_read(self):
        with tempfile.TemporaryDirectory() as d:
            single = self._run(f"{d}/single.pq")
            with mock.patch.object(const, "INGEST_BATCH_ROWS", 5):
                batched = self._run(f"{d}/batched.pq")
        self.assertEqual(
            set(zip(single.index, single["fid"], strict=True)),
            set(zip(batched.index, batched["fid"], strict=True)),
        )
        # synthetic fids assigned across batches collide with nothing
        self.assertEqual(
            sorted(single["fid"].unique()), sorted(batched["fid"].unique())
        )
