from classes.base import TestRunthrough
from data.datapaths import *

from vector2dggs.s2 import s2


class TestS2(TestRunthrough):
    """
    Sends the test data file through S2 indexing using default parameters.
    """

    def test_s2_run(self):
        try:
            s2(
                [TEST_FILE_PATH, str(TEST_OUTPUT_PATH), "-r", "13"],
                standalone_mode=False,
            )

        except Exception:
            self.fail(f"S2 runthrough failed.")

    def test_s2_run_overwrite(self):
        try:
            s2(
                [TEST_FILE_PATH, str(TEST_OUTPUT_PATH), "-r", "13"],
                standalone_mode=False,
            )
            s2(
                [TEST_FILE_PATH, str(TEST_OUTPUT_PATH), "-r", "13", "-o"],
                standalone_mode=False,
            )

        except Exception:
            self.fail(f"S2 runthrough with overwrite failed.")

    def test_s2_cut_crs(self):
        try:
            s2(
                [
                    TEST_FILE_PATH,
                    str(TEST_OUTPUT_PATH),
                    "-r",
                    "13",
                    "-crs",
                    "3793",
                    "-c",
                    "4000",
                ],
                standalone_mode=False,
            )

        except Exception:
            self.fail("S2 run through using actual CRS failed")

    def test_s2_cut_crs_reproject(self):
        try:
            s2(
                [
                    TEST_FILE_PATH,
                    str(TEST_OUTPUT_PATH),
                    "-r",
                    "13",
                    "-crs",
                    "4326",
                    "-c",
                    "0.005",
                ],
                standalone_mode=False,
            )
        except Exception:
            self.fail("S2 run through with reprojected CRS failed")

    def test_s2_no_bisection(self):
        try:
            s2(
                [
                    TEST_FILE_PATH,
                    str(TEST_OUTPUT_PATH),
                    "-r",
                    "13",
                    "-c",
                    "0",
                ],
                standalone_mode=False,
            )
        except Exception:
            self.fail("S2 run through without bisection failed")
