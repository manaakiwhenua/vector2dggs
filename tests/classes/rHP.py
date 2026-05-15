from .base import TestRunthrough
from ..data.datapaths import *

from vector2dggs.rHP import rhp


class TestRHP(TestRunthrough):
    """
    Sends the test data file through rHP indexing using default parameters.
    """

    def test_rhp_run(self):
        rhp(
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

    def test_rhp_run_overwrite(self):
        rhp(
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
        rhp(
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

    def test_rhp_cut_crs(self):
        rhp(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "8",
                "-crs",
                "3793",
            ],
            standalone_mode=False,
        )

    def test_rhp_cut_crs_reproject(self):
        rhp(
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

    def test_rhp_no_bisection(self):
        rhp(
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

    def test_rhp_compaction(self):
        rhp(
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

    def test_rhp_geo_point(self):
        rhp(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "8",
                "--geo",
                "point",
            ],
            standalone_mode=False,
        )

    def test_rhp_geo_point_compact(self):
        rhp(
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

    def test_rhp_geo_polygon(self):
        rhp(
            [
                TEST_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LAYER_NAME,
                "-r",
                "8",
                "--geo",
                "polygon",
            ],
            standalone_mode=False,
        )

    def test_rhp_geo_polygon_compact(self):
        rhp(
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

    def test_rhp_linestring_run(self):
        rhp(
            [
                TEST_LINESTRING_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LINESTRING_LAYER_NAME,
                "-r",
                "8",
                "-c",
                "0",
            ],
            standalone_mode=False,
        )

    def test_rhp_linestring_keep_attrs(self):
        rhp(
            [
                TEST_LINESTRING_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LINESTRING_LAYER_NAME,
                "-r",
                "8",
                "-c",
                "0",
                "-k",
            ],
            standalone_mode=False,
        )

    def test_rhp_linestring_compaction(self):
        rhp(
            [
                TEST_LINESTRING_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LINESTRING_LAYER_NAME,
                "-r",
                "8",
                "-c",
                "0",
                "-co",
                "-id",
                "t50_fid",
            ],
            standalone_mode=False,
        )

    def test_rhp_linestring_geo_point(self):
        rhp(
            [
                TEST_LINESTRING_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LINESTRING_LAYER_NAME,
                "-r",
                "8",
                "-c",
                "0",
                "--geo",
                "point",
            ],
            standalone_mode=False,
        )

    def test_rhp_linestring_geo_polygon(self):
        rhp(
            [
                TEST_LINESTRING_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_LINESTRING_LAYER_NAME,
                "-r",
                "8",
                "-c",
                "0",
                "--geo",
                "polygon",
            ],
            standalone_mode=False,
        )

    def test_rhp_point_run(self):
        rhp(
            [
                TEST_POINT_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_POINT_LAYER_NAME,
                "-r",
                "8",
                "-c",
                "0",
            ],
            standalone_mode=False,
        )

    def test_rhp_point_keep_attrs(self):
        rhp(
            [
                TEST_POINT_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_POINT_LAYER_NAME,
                "-r",
                "8",
                "-c",
                "0",
                "-k",
            ],
            standalone_mode=False,
        )

    def test_rhp_point_compaction(self):
        rhp(
            [
                TEST_POINT_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_POINT_LAYER_NAME,
                "-r",
                "8",
                "-c",
                "0",
                "-co",
                "-id",
                "t50_fid",
            ],
            standalone_mode=False,
        )

    def test_rhp_point_geo_point(self):
        rhp(
            [
                TEST_POINT_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_POINT_LAYER_NAME,
                "-r",
                "8",
                "-c",
                "0",
                "--geo",
                "point",
            ],
            standalone_mode=False,
        )

    def test_rhp_point_geo_polygon(self):
        rhp(
            [
                TEST_POINT_FILE_PATH,
                str(TEST_OUTPUT_PATH),
                "--layer",
                TEST_POINT_LAYER_NAME,
                "-r",
                "8",
                "-c",
                "0",
                "--geo",
                "polygon",
            ],
            standalone_mode=False,
        )
