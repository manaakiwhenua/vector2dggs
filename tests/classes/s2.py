from .base import TestRunthrough
from ..data.datapaths import *

from vector2dggs.s2 import s2


class TestS2(TestRunthrough):
    """
    Sends the test data file through S2 indexing using default parameters.
    """

    def test_s2_run(self):
        s2(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "13",
            ],
            standalone_mode=False,
        )

    def test_s2_run_overwrite(self):
        s2(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "13",
            ],
            standalone_mode=False,
        )
        s2(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "13",
                "-o",
            ],
            standalone_mode=False,
        )

    def test_s2_cut_crs(self):
        s2(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "13",
                "-crs",
                "3793",
                "-c",
                "4000",
            ],
            standalone_mode=False,
        )

    def test_s2_cut_crs_reproject(self):
        s2(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "13",
                "-crs",
                "4326",
                "-c",
                "0.005",
            ],
            standalone_mode=False,
        )

    def test_s2_no_bisection(self):
        s2(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "13",
                "-c",
                "0",
            ],
            standalone_mode=False,
        )

    def test_s2_compaction(self):
        s2(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "13",
                "-co",
                "-id",
                "LCDB_UID",
            ],
            standalone_mode=False,
        )

    def test_s2_geo_point(self):
        s2(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "13",
                "--geo",
                "point",
            ],
            standalone_mode=False,
        )

    def test_s2_geo_point_compact(self):
        s2(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "13",
                "--geo",
                "point",
                "-co",
                "-id",
                "LCDB_UID",
                "-o",
            ],
            standalone_mode=False,
        )

    def test_s2_geo_polygon(self):
        s2(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "13",
                "--geo",
                "polygon",
            ],
            standalone_mode=False,
        )

    def test_s2_geo_polygon_compact(self):
        s2(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "13",
                "--geo",
                "polygon",
                "-co",
                "-id",
                "LCDB_UID",
                "-o",
            ],
            standalone_mode=False,
        )

    def test_s2_linestring_run(self):
        s2(
            [
                TEST_LINESTRING_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LINESTRING_LAYER_NAME,
                "-r",
                "13",
                "-c",
                "0",
            ],
            standalone_mode=False,
        )

    def test_s2_linestring_keep_attrs(self):
        s2(
            [
                TEST_LINESTRING_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LINESTRING_LAYER_NAME,
                "-r",
                "13",
                "-c",
                "0",
                "-k",
            ],
            standalone_mode=False,
        )

    def test_s2_linestring_compaction(self):
        s2(
            [
                TEST_LINESTRING_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LINESTRING_LAYER_NAME,
                "-r",
                "13",
                "-c",
                "0",
                "-co",
                "-id",
                "t50_fid",
            ],
            standalone_mode=False,
        )

    def test_s2_linestring_geo_point(self):
        s2(
            [
                TEST_LINESTRING_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LINESTRING_LAYER_NAME,
                "-r",
                "13",
                "-c",
                "0",
                "--geo",
                "point",
            ],
            standalone_mode=False,
        )

    def test_s2_linestring_geo_polygon(self):
        s2(
            [
                TEST_LINESTRING_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LINESTRING_LAYER_NAME,
                "-r",
                "13",
                "-c",
                "0",
                "--geo",
                "polygon",
            ],
            standalone_mode=False,
        )

    def test_s2_point_run(self):
        s2(
            [
                TEST_POINT_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_POINT_LAYER_NAME,
                "-r",
                "13",
                "-c",
                "0",
            ],
            standalone_mode=False,
        )

    def test_s2_point_keep_attrs(self):
        s2(
            [
                TEST_POINT_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_POINT_LAYER_NAME,
                "-r",
                "13",
                "-c",
                "0",
                "-k",
            ],
            standalone_mode=False,
        )

    def test_s2_point_compaction(self):
        s2(
            [
                TEST_POINT_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_POINT_LAYER_NAME,
                "-r",
                "13",
                "-c",
                "0",
                "-co",
                "-id",
                "t50_fid",
            ],
            standalone_mode=False,
        )

    def test_s2_point_geo_point(self):
        s2(
            [
                TEST_POINT_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_POINT_LAYER_NAME,
                "-r",
                "13",
                "-c",
                "0",
                "--geo",
                "point",
            ],
            standalone_mode=False,
        )

    def test_s2_point_geo_polygon(self):
        s2(
            [
                TEST_POINT_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_POINT_LAYER_NAME,
                "-r",
                "13",
                "-c",
                "0",
                "--geo",
                "polygon",
            ],
            standalone_mode=False,
        )
