import tempfile
import warnings
from unittest import TestCase, mock

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

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
            1,
            layer=TEST_LAYER_NAME,
            compact=False,
        )
        return pd.read_parquet(out)

    def test_batches_equivalent_to_single_read(self):
        with tempfile.TemporaryDirectory() as d:
            single = self._run(f"{d}/single.pq")
            with (
                mock.patch.object(const, "INGEST_PROBE_ROWS", 5),
                mock.patch.object(const, "TARGET_BATCH_BYTES", 1),
            ):
                batched = self._run(f"{d}/batched.pq")
        self.assertEqual(
            set(zip(single.index, single["fid"], strict=True)),
            set(zip(batched.index, batched["fid"], strict=True)),
        )
        # synthetic fids assigned across batches collide with nothing
        self.assertEqual(
            sorted(single["fid"].unique()), sorted(batched["fid"].unique())
        )


class TestDefaultLayer(TestCase):
    """A multi-layer input indexed without --layer must announce which layer
    it chose in the tool's own log voice, once, instead of leaving it to
    pyogrio's easily-missed UserWarning; single-layer inputs stay quiet."""

    def _write_multi(self, path):
        first = gpd.GeoDataFrame({"geometry": [Point(174.0, -41.0)]}, crs=4326)
        second = gpd.GeoDataFrame(
            {"geometry": [Point(10.0, 10.0), Point(11.0, 11.0)]}, crs=4326
        )
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            first.to_file(path, layer="zebra")
            second.to_file(path, layer="alpha")

    def _index(self, src, out):
        return common.index("h3", src, out, 5, None, False, 1, compact=False)

    def test_multi_layer_default_is_announced_and_first_layer_indexed(self):
        with tempfile.TemporaryDirectory() as d:
            self._write_multi(f"{d}/multi.gpkg")
            with self.assertLogs(common.LOGGER, level="WARNING") as logs:
                self._index(f"{d}/multi.gpkg", f"{d}/out.pq")
            message = "\n".join(logs.output)
            self.assertIn("zebra", message)
            self.assertIn("alpha", message)
            self.assertIn("--layer", message)
            # the default (first) layer, and only it, was indexed
            out = pd.read_parquet(f"{d}/out.pq")
            self.assertEqual(out["fid"].nunique(), 1)

    def test_pyogrio_multi_layer_warning_is_superseded(self):
        with tempfile.TemporaryDirectory() as d:
            self._write_multi(f"{d}/multi.gpkg")
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                self._index(f"{d}/multi.gpkg", f"{d}/out.pq")
        self.assertFalse([w for w in caught if "More than one layer" in str(w.message)])

    def test_single_layer_stays_quiet(self):
        with tempfile.TemporaryDirectory() as d:
            df = gpd.GeoDataFrame({"geometry": [Point(174.0, -41.0)]}, crs=4326)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                df.to_file(f"{d}/single.gpkg", layer="only")
            with self.assertNoLogs(common.LOGGER, level="WARNING"):
                self._index(f"{d}/single.gpkg", f"{d}/out.pq")
