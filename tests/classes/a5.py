from .base import TestRunthrough
from ..data.datapaths import *

from vector2dggs.a5 import a5


class TestA5(TestRunthrough):
    """
    Sends the test data file through A5 indexing using default parameters.
    """

    def test_a5_run(self):
        try:
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

        except Exception:
            self.fail(f"A5 runthrough failed.")

    def test_a5_run_overwrite(self):
        try:
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

        except Exception:
            self.fail(f"A5 runthrough with overwrite failed.")

    def test_a5_cut_crs(self):
        try:
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

        except Exception:
            self.fail("A5 run through using actual CRS failed")

    def test_a5_cut_crs_reproject(self):
        try:
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
        except Exception:
            self.fail("A5 run through with reprojected CRS failed")

    def test_a5_no_bisection(self):
        try:
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
        except Exception:
            self.fail("A5 run through without bisection failed")

    def test_a5_compaction(self):
        try:
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

        except Exception:
            self.fail(f"A5 runthrough with compaction failed.")

    def test_a5_geo_point(self):
        try:
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
        except Exception:
            self.fail("A5 run through with --geo point failed")

    def test_a5_geo_point_compact(self):
        try:
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
        except Exception:
            self.fail("A5 run through with --geo point -co failed")

    def test_a5_geo_polygon(self):
        try:
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
        except Exception:
            self.fail("A5 run through with --geo polygon failed")

    def test_a5_geo_polygon_compact(self):
        try:
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
        except Exception:
            self.fail("A5 run through with --geo polygon -co failed")
