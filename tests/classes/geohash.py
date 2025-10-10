from classes.base import TestRunthrough
from data.datapaths import *

from vector2dggs.geohash import geohash


class TestGeohash(TestRunthrough):
    """
    Sends the test data file through Geohash indexing using default parameters.
    """

    def test_geohash_run(self):
        try:
            geohash(
                [TEST_FILE_PATH, str(TEST_OUTPUT_PATH), "-r", "6"],
                standalone_mode=False,
            )

        except Exception:
            self.fail(f"TestGeohash.test_geohash_run: Geohash runthrough failed.")

    def test_geohash_run_overwrite(self):
        try:
            geohash(
                [TEST_FILE_PATH, str(TEST_OUTPUT_PATH), "-r", "6"],
                standalone_mode=False,
            )
            geohash(
                [TEST_FILE_PATH, str(TEST_OUTPUT_PATH), "-r", "6", "-o"],
                standalone_mode=False,
            )

        except Exception:
            self.fail(f"geohash runthrough with overwrite failed.")

    def test_geohash_cut_crs(self):
        try:
            geohash(
                [TEST_FILE_PATH, str(TEST_OUTPUT_PATH), "-r", "6", "-crs", "3793"],
                standalone_mode=False,
            )

        except Exception:
            self.fail("geohash run through using actual CRS failed")

    def test_geohash_cut_crs_reproject(self):
        try:
            geohash(
                [
                    TEST_FILE_PATH,
                    str(TEST_OUTPUT_PATH),
                    "-r",
                    "6",
                    "-crs",
                    "4326",
                    "-c",
                    "0.005",
                ],
                standalone_mode=False,
            )
        except Exception:
            self.fail("geohash run through with reprojected CRS failed")
