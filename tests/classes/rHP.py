from .base import TestRunthrough
from ..data.datapaths import *

from vector2dggs.rHP import rhp


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
            self.fail("rHP run through with reprojected CRS failed")

    def test_rhp_compaction(self):
        try:
            rhp(
                [
                    TEST_FILE_PATH,
                    str(TEST_OUTPUT_PATH),
                    "-r",
                    "8",
                    "-co",
                    "-id",
                    "LCDB_UID",
                ],
                standalone_mode=False,
            )

        except Exception:
            self.fail(f"rHP runthrough failed.")

    def test_rhp_geo_point(self):
        try:
            rhp(
                [
                    TEST_FILE_PATH,
                    str(TEST_OUTPUT_PATH),
                    "-r",
                    "8",
                    "--geo",
                    "point",
                ],
                standalone_mode=False,
            )
        except Exception:
            self.fail("rHP run through with geo point failed")

    def test_rhp_geo_point_compact(self):
        try:
            rhp(
                [
                    TEST_FILE_PATH,
                    str(TEST_OUTPUT_PATH),
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
        except Exception:
            self.fail("rHP run through with geo point compact failed")

    def test_rhp_geo_polygon(self):
        try:
            rhp(
                [
                    TEST_FILE_PATH,
                    str(TEST_OUTPUT_PATH),
                    "-r",
                    "8",
                    "--geo",
                    "polygon",
                ],
                standalone_mode=False,
            )
        except Exception:
            self.fail("rHP run through with geo polygon failed")

    def test_rhp_geo_polygon_compact(self):
        try:
            rhp(
                [
                    TEST_FILE_PATH,
                    str(TEST_OUTPUT_PATH),
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
        except Exception:
            self.fail("rHP run through with geo polygon compact failed")
