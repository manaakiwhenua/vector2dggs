from .base import TestRunthrough
from ..data.datapaths import *

from vector2dggs.geohash import geohash


class TestGeohash(TestRunthrough):
    """
    Sends the test data file through Geohash indexing using default parameters.
    """

    def test_geohash_run(self):
        geohash(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "6",
            ],
            standalone_mode=False,
        )

    def test_geohash_run_overwrite(self):
        geohash(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "6",
            ],
            standalone_mode=False,
        )
        geohash(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "6",
                "-o",
            ],
            standalone_mode=False,
        )

    def test_geohash_cut_crs(self):
        geohash(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "6",
                "-crs",
                "3793",
            ],
            standalone_mode=False,
        )

    def test_geohash_cut_crs_reproject(self):
        geohash(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "6",
                "-crs",
                "4326",
                "-c",
                "0.005",
            ],
            standalone_mode=False,
        )

    def test_geohash_no_bisection(self):
        geohash(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "6",
                "-c",
                "0",
            ],
            standalone_mode=False,
        )

    def test_geohash_compaction(self):
        geohash(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "6",
                "-co",
                "-id",
                "LCDB_UID",
            ],
            standalone_mode=False,
        )

    def test_geohash_geo_point(self):
        geohash(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "6",
                "--geo",
                "point",
            ],
            standalone_mode=False,
        )

    def test_geohash_geo_point_compact(self):
        geohash(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "6",
                "--geo",
                "point",
                "-co",
                "-id",
                "LCDB_UID",
                "-o",
            ],
            standalone_mode=False,
        )

    def test_geohash_geo_polygon(self):
        geohash(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "6",
                "--geo",
                "polygon",
            ],
            standalone_mode=False,
        )

    def test_geohash_geo_polygon_compact(self):
        geohash(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "6",
                "--geo",
                "polygon",
                "-co",
                "-id",
                "LCDB_UID",
                "-o",
            ],
            standalone_mode=False,
        )

    def test_geohash_linestring_run(self):
        geohash(
            [
                TEST_LINESTRING_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LINESTRING_LAYER_NAME,
                "-r",
                "6",
                "-c",
                "0",
            ],
            standalone_mode=False,
        )

    def test_geohash_linestring_keep_attrs(self):
        geohash(
            [
                TEST_LINESTRING_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LINESTRING_LAYER_NAME,
                "-r",
                "6",
                "-c",
                "0",
                "-k",
            ],
            standalone_mode=False,
        )

    def test_geohash_linestring_compaction(self):
        geohash(
            [
                TEST_LINESTRING_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LINESTRING_LAYER_NAME,
                "-r",
                "6",
                "-c",
                "0",
                "-co",
                "-id",
                "t50_fid",
            ],
            standalone_mode=False,
        )

    def test_geohash_linestring_geo_point(self):
        geohash(
            [
                TEST_LINESTRING_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LINESTRING_LAYER_NAME,
                "-r",
                "6",
                "-c",
                "0",
                "--geo",
                "point",
            ],
            standalone_mode=False,
        )

    def test_geohash_linestring_geo_polygon(self):
        geohash(
            [
                TEST_LINESTRING_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LINESTRING_LAYER_NAME,
                "-r",
                "6",
                "-c",
                "0",
                "--geo",
                "polygon",
            ],
            standalone_mode=False,
        )

    def test_geohash_point_run(self):
        geohash(
            [
                TEST_POINT_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_POINT_LAYER_NAME,
                "-r",
                "6",
                "-c",
                "0",
            ],
            standalone_mode=False,
        )

    def test_geohash_point_keep_attrs(self):
        geohash(
            [
                TEST_POINT_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_POINT_LAYER_NAME,
                "-r",
                "6",
                "-c",
                "0",
                "-k",
            ],
            standalone_mode=False,
        )

    def test_geohash_point_compaction(self):
        geohash(
            [
                TEST_POINT_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_POINT_LAYER_NAME,
                "-r",
                "6",
                "-c",
                "0",
                "-co",
                "-id",
                "t50_fid",
            ],
            standalone_mode=False,
        )

    def test_geohash_point_geo_point(self):
        geohash(
            [
                TEST_POINT_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_POINT_LAYER_NAME,
                "-r",
                "6",
                "-c",
                "0",
                "--geo",
                "point",
            ],
            standalone_mode=False,
        )

    def test_geohash_point_geo_polygon(self):
        geohash(
            [
                TEST_POINT_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_POINT_LAYER_NAME,
                "-r",
                "6",
                "-c",
                "0",
                "--geo",
                "polygon",
            ],
            standalone_mode=False,
        )
