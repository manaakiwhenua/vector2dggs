from vector2dggs.h3 import h3

from ..data.datapaths import (
    TEST_LINESTRING_FILE_PATH,
    TEST_LINESTRING_LAYER_NAME,
    TEST_OUTPUT_PATH,
    TEST_POINT_FILE_PATH,
    TEST_POINT_LAYER_NAME,
)
from .base import TestRunthrough, skip_unless_backend


class TestGeometryTypes(TestRunthrough):
    """
    Verifies that LineString and Point geometry types are indexed end-to-end.
    Uses H3 as the reference backend. Bisection is disabled (-c 0) since the
    fixtures are small and these tests are purely about geometry-type routing.
    """

    @classmethod
    def setUpClass(cls):
        skip_unless_backend("h3")
        super().setUpClass()

    def test_h3_linestring(self):
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

    def test_h3_point(self):
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
