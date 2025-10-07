"""
@author: ndemaio
"""

from unittest import *
from data.datapaths import *

from vector2dggs.h3 import h3
from vector2dggs.rHP import rhp
from vector2dggs.s2 import s2
from vector2dggs.geohash import geohash


class TestRunthrough(TestCase):
    """
    Parent class for the smoke tests. Handles temporary output files by
    overriding the built in setup and teardown methods from TestCase. Provides
    two new member functions to recurse through nested output folders to empty
    them.
    """

    def setUp(self):
        self.checkAndClearOutput(TEST_OUTPUT_PATH)

    def tearDown(self):
        self.checkAndClearOutput(TEST_OUTPUT_PATH)

    def checkAndClearOutput(self, folder):
        if folder.exists():
            self.clearOutput(folder)
            folder.rmdir()

    def clearOutput(self, folder):
        for child in folder.iterdir():
            if child.is_dir():
                self.clearOutput(child)
                child.rmdir()
            else:
                child.unlink()


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
            self.fail("H3 run through with reporjected CRS failed")


class TestRHP(TestRunthrough):
    """
    Sends the test data file through rHP indexing using default parameters.
    """

    def test_rhp_run(self):
        try:
            rhp(
                [TEST_FILE_PATH, str(TEST_OUTPUT_PATH), "-r", "8"],
                standalone_mode=False,
            )

        except Exception:
            self.fail(f"rHP runthrough failed.")

    def test_rhp_run_overwrite(self):
        try:
            rhp(
                [TEST_FILE_PATH, str(TEST_OUTPUT_PATH), "-r", "8"],
                standalone_mode=False,
            )
            rhp(
                [TEST_FILE_PATH, str(TEST_OUTPUT_PATH), "-r", "8", "-o"],
                standalone_mode=False,
            )

        except Exception:
            self.fail(f"rHP runthrough with overwrite failed.")

    def test_rhp_cut_crs(self):
        try:
            rhp(
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
            self.fail("rHP run through using actual CRS failed")

    def test_rhp_cut_crs_reproject(self):
        try:
            rhp(
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
            self.fail("rHP run through with reporjected CRS failed")


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
            self.fail("S2 run through with reporjected CRS failed")


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

    def test_sgeohash_cut_crs(self):
        try:
            geohash(
                [
                    TEST_FILE_PATH,
                    str(TEST_OUTPUT_PATH),
                    "-r",
                    "6",
                    "-crs",
                    "3793",
                    "-c",
                    "4000",
                ],
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
            self.fail("geohash run through with reporjected CRS failed")
