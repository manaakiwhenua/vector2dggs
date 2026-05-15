from .base import TestRunthrough
from ..data.datapaths import *

from vector2dggs.a5 import a5


class TestA5(TestRunthrough):
    """
    Sends the test data file through A5 indexing using default parameters.
    """

    def test_a5_run(self):
        a5(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "17",
            ],
            standalone_mode=False,
        )

    def test_a5_run_overwrite(self):
        a5(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "17",
            ],
            standalone_mode=False,
        )
        a5(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "17",
                "-o",
            ],
            standalone_mode=False,
        )

    def test_a5_cut_crs(self):
        a5(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "17",
                "-crs",
                "3793",
                "-c",
                "4000",
            ],
            standalone_mode=False,
        )

    def test_a5_cut_crs_reproject(self):
        a5(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "17",
                "-crs",
                "4326",
                "-c",
                "0.005",
            ],
            standalone_mode=False,
        )

    def test_a5_no_bisection(self):
        a5(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "17",
                "-c",
                "0",
            ],
            standalone_mode=False,
        )

    def test_a5_compaction(self):
        a5(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "17",
                "-co",
                "-id",
                "LCDB_UID",
            ],
            standalone_mode=False,
        )

    def test_a5_geo_point(self):
        a5(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "17",
                "--geo",
                "point",
            ],
            standalone_mode=False,
        )

    def test_a5_geo_point_compact(self):
        a5(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "17",
                "--geo",
                "point",
                "-co",
                "-id",
                "LCDB_UID",
                "-o",
            ],
            standalone_mode=False,
        )

    def test_a5_geo_polygon(self):
        a5(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "17",
                "--geo",
                "polygon",
            ],
            standalone_mode=False,
        )

    def test_a5_geo_polygon_compact(self):
        a5(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "17",
                "--geo",
                "polygon",
                "-co",
                "-id",
                "LCDB_UID",
                "-o",
            ],
            standalone_mode=False,
        )

    def test_a5_linestring_run(self):
        a5(
            [
                TEST_LINESTRING_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LINESTRING_LAYER_NAME,
                "-r",
                "17",
                "-c",
                "0",
            ],
            standalone_mode=False,
        )

    def test_a5_linestring_keep_attrs(self):
        a5(
            [
                TEST_LINESTRING_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LINESTRING_LAYER_NAME,
                "-r",
                "17",
                "-c",
                "0",
                "-k",
            ],
            standalone_mode=False,
        )

    def test_a5_linestring_compaction(self):
        a5(
            [
                TEST_LINESTRING_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LINESTRING_LAYER_NAME,
                "-r",
                "17",
                "-c",
                "0",
                "-co",
                "-id",
                "t50_fid",
            ],
            standalone_mode=False,
        )

    def test_a5_linestring_geo_point(self):
        a5(
            [
                TEST_LINESTRING_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LINESTRING_LAYER_NAME,
                "-r",
                "17",
                "-c",
                "0",
                "--geo",
                "point",
            ],
            standalone_mode=False,
        )

    def test_a5_linestring_geo_polygon(self):
        a5(
            [
                TEST_LINESTRING_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LINESTRING_LAYER_NAME,
                "-r",
                "17",
                "-c",
                "0",
                "--geo",
                "polygon",
            ],
            standalone_mode=False,
        )

    def test_a5_point_run(self):
        a5(
            [
                TEST_POINT_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_POINT_LAYER_NAME,
                "-r",
                "17",
                "-c",
                "0",
            ],
            standalone_mode=False,
        )

    def test_a5_point_keep_attrs(self):
        a5(
            [
                TEST_POINT_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_POINT_LAYER_NAME,
                "-r",
                "17",
                "-c",
                "0",
                "-k",
            ],
            standalone_mode=False,
        )

    def test_a5_point_compaction(self):
        a5(
            [
                TEST_POINT_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_POINT_LAYER_NAME,
                "-r",
                "17",
                "-c",
                "0",
                "-co",
                "-id",
                "t50_fid",
            ],
            standalone_mode=False,
        )

    def test_a5_point_geo_point(self):
        a5(
            [
                TEST_POINT_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_POINT_LAYER_NAME,
                "-r",
                "17",
                "-c",
                "0",
                "--geo",
                "point",
            ],
            standalone_mode=False,
        )

    def test_a5_point_geo_polygon(self):
        a5(
            [
                TEST_POINT_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_POINT_LAYER_NAME,
                "-r",
                "17",
                "-c",
                "0",
                "--geo",
                "polygon",
            ],
            standalone_mode=False,
        )
