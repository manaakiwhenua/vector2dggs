import tempfile
from pathlib import Path
from unittest import TestCase

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

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


class TestDictionaryEncodeAttributes(TestCase):
    """
    _dictionary_encode_attributes casts string attribute columns to category
    dtype (see issue #181: --keep_attributes duplicates the full attribute
    payload onto every generated cell; dictionary encoding turns repeated
    values into small integer codes referencing one shared dictionary,
    rather than independent string copies per cell).
    """

    def test_object_columns_become_categorical(self):
        gdf = gpd.GeoDataFrame(
            {
                "geometry": [Point(0, 0), Point(1, 1)],
                "class": ["forest", "wetland"],
                "count": [1, 2],
            }
        )
        result = common._dictionary_encode_attributes(gdf)
        self.assertEqual(result["class"].dtype, "category")
        self.assertEqual(list(result["class"]), ["forest", "wetland"])

    def test_numeric_and_geometry_columns_untouched(self):
        gdf = gpd.GeoDataFrame({"geometry": [Point(0, 0)], "count": [1]})
        result = common._dictionary_encode_attributes(gdf)
        self.assertEqual(result["count"].dtype, gdf["count"].dtype)
        self.assertEqual(result.geometry.name, "geometry")

    def test_no_string_columns_is_a_no_op(self):
        gdf = gpd.GeoDataFrame({"geometry": [Point(0, 0)], "count": [1]})
        result = common._dictionary_encode_attributes(gdf)
        pd.testing.assert_frame_equal(result, gdf)

    def test_prepare_dataframe_encodes_only_when_keeping_attributes(self):
        gdf = gpd.GeoDataFrame({"geometry": [Point(0, 0)], "class": ["forest"]})

        kept = common._prepare_dataframe(gdf.copy(), None, keep_attributes=True)
        self.assertEqual(kept["class"].dtype, "category")

        dropped = common._prepare_dataframe(gdf.copy(), None, keep_attributes=False)
        self.assertNotIn("class", dropped.columns)


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
