from .base import TestRunthrough
from ..data.datapaths import *

from vector2dggs.h3 import h3


class TestH3(TestRunthrough):
    """
    Sends the test data file through H3 indexing using default parameters.
    """

    def test_h3_run(self):
        h3(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "8",
            ],
            standalone_mode=False,
        )

    def test_h3_run_overwrite(self):
        h3(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "8",
            ],
            standalone_mode=False,
        )
        h3(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "8",
                "-o",
            ],
            standalone_mode=False,
        )

    def test_h3_cut_crs(self):
        h3(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "8",
                "-crs",
                "3793",
                "-c",
                "4000",
            ],
            standalone_mode=False,
        )

    def test_h3_cut_crs_reproject(self):
        h3(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "8",
                "-crs",
                "4326",
                "-c",
                "0.005",
            ],
            standalone_mode=False,
        )

    def test_h3_no_bisection(self):
        h3(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "8",
                "-c",
                "0",
            ],
            standalone_mode=False,
        )

    def test_h3_compaction(self):
        h3(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "8",
                "-co",
                "-id",
                "LCDB_UID",
            ],
            standalone_mode=False,
        )

    def test_h3_geo_point(self):
        h3(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "8",
                "--geo",
                "point",
                "-o",
            ],
            standalone_mode=False,
        )

    def test_h3_geo_point_compact(self):
        h3(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "8",
                "--geo",
                "point",
                "-co",
                "-id",
                "LCDB_UID",
                "-o",
            ],
            standalone_mode=False,
        )

    def test_h3_geo_polygon(self):
        h3(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "8",
                "--geo",
                "polygon",
                "-o",
            ],
            standalone_mode=False,
        )

    def test_h3_geo_polygon_compact(self):
        h3(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "8",
                "--geo",
                "polygon",
                "-co",
                "-id",
                "LCDB_UID",
                "-o",
            ],
            standalone_mode=False,
        )

    def test_h3_linestring_run(self):
        h3(
            [
                TEST_LINESTRING_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LINESTRING_LAYER_NAME,
                "-r",
                "10",
                "-c",
                "0",
            ],
            standalone_mode=False,
        )

    def test_h3_linestring_keep_attrs(self):
        h3(
            [
                TEST_LINESTRING_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LINESTRING_LAYER_NAME,
                "-r",
                "10",
                "-c",
                "0",
                "-k",
            ],
            standalone_mode=False,
        )

    def test_h3_linestring_compaction(self):
        h3(
            [
                TEST_LINESTRING_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LINESTRING_LAYER_NAME,
                "-r",
                "10",
                "-c",
                "0",
                "-co",
                "-id",
                "t50_fid",
            ],
            standalone_mode=False,
        )

    def test_h3_linestring_geo_point(self):
        h3(
            [
                TEST_LINESTRING_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LINESTRING_LAYER_NAME,
                "-r",
                "10",
                "-c",
                "0",
                "--geo",
                "point",
            ],
            standalone_mode=False,
        )

    def test_h3_linestring_geo_polygon(self):
        h3(
            [
                TEST_LINESTRING_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LINESTRING_LAYER_NAME,
                "-r",
                "10",
                "-c",
                "0",
                "--geo",
                "polygon",
            ],
            standalone_mode=False,
        )

    def test_h3_point_run(self):
        h3(
            [
                TEST_POINT_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_POINT_LAYER_NAME,
                "-r",
                "10",
                "-c",
                "0",
            ],
            standalone_mode=False,
        )

    def test_h3_point_keep_attrs(self):
        h3(
            [
                TEST_POINT_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_POINT_LAYER_NAME,
                "-r",
                "10",
                "-c",
                "0",
                "-k",
            ],
            standalone_mode=False,
        )

    def test_h3_point_compaction(self):
        h3(
            [
                TEST_POINT_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_POINT_LAYER_NAME,
                "-r",
                "10",
                "-c",
                "0",
                "-co",
                "-id",
                "t50_fid",
            ],
            standalone_mode=False,
        )

    def test_h3_point_geo_point(self):
        h3(
            [
                TEST_POINT_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_POINT_LAYER_NAME,
                "-r",
                "10",
                "-c",
                "0",
                "--geo",
                "point",
            ],
            standalone_mode=False,
        )

    def test_h3_point_geo_polygon(self):
        h3(
            [
                TEST_POINT_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_POINT_LAYER_NAME,
                "-r",
                "10",
                "-c",
                "0",
                "--geo",
                "polygon",
            ],
            standalone_mode=False,
        )
