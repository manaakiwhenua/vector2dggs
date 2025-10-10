from classes.base import TestRunthrough
from data.datapaths import *

from vector2dggs.h3 import h3


class TestH3(TestRunthrough):
    """
    Sends the test data file through H3 indexing using default parameters.
    """

    def test_h3_run(self):
        try:
            h3(
                [TEST_FILE_PATH, str(TEST_OUTPUT_PATH), "-r", "8"],
                standalone_mode=False,
            )

        except Exception:
            self.fail(f"H3 runthrough failed.")

    def test_h3_run_overwrite(self):
        try:
            h3(
                [TEST_FILE_PATH, str(TEST_OUTPUT_PATH), "-r", "8"],
                standalone_mode=False,
            )
            h3(
                [TEST_FILE_PATH, str(TEST_OUTPUT_PATH), "-r", "8", "-o"],
                standalone_mode=False,
            )

        except Exception:
            self.fail(f"H3 runthrough with overwrite failed.")

    def test_h3_cut_crs(self):
        try:
            h3(
                [
                    TEST_FILE_PATH,
                    str(TEST_OUTPUT_PATH),
                    "-r",
                    "8",
                    "-crs",
                    "3793",
                    "-c",
                    "4000",
                ],
                standalone_mode=False,
            )

        except Exception:
            self.fail("H3 run through using actual CRS failed")

    def test_h3_cut_crs_reproject(self):
        try:
            h3(
                [
                    TEST_FILE_PATH,
                    str(TEST_OUTPUT_PATH),
                    "-r",
                    "8",
                    "-crs",
                    "4326",
                    "-c",
                    "0.005",
                ],
                standalone_mode=False,
            )
        except Exception:
            self.fail("H3 run through with reprojected CRS failed")

    def test_h3_no_bisection(self):
        try:
            h3(
                [
                    TEST_FILE_PATH,
                    str(TEST_OUTPUT_PATH),
                    "-r",
                    "8",
                    "-c",
                    "0",
                ],
                standalone_mode=False,
            )
        except Exception:
            self.fail("H3 run through without bisection failed")
