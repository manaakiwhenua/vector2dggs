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
    a new member function to recurse through nested output folders to empty
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
            self.fail(f"TestH3.test_h3_run: H3 runthrough failed.")


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
            self.fail(f"TestRHP.test_rhp_run: rHP runthrough failed.")


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
            self.fail(f"TestS2.test_s2_run: S2 runthrough failed.")


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
