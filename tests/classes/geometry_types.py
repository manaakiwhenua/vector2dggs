from .base import TestRunthrough
from ..data.datapaths import *

from vector2dggs.h3 import h3


class TestGeometryTypes(TestRunthrough):
    """
    Verifies that LineString and Point geometry types are indexed end-to-end.
    Uses H3 as the reference backend. Bisection is disabled (-c 0) since the
    fixtures are small and these tests are purely about geometry-type routing.
    """

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
