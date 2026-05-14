from unittest import TestCase

from vector2dggs import common
from vector2dggs.h3 import h3
from vector2dggs.indexerfactory import indexer_instance

from .base import TestRunthrough
from ..data.datapaths import *


class TestErrors(TestCase):
    """
    Error-path unit tests that raise before touching the filesystem,
    so no output cleanup is needed.
    """

    def test_parent_res_not_less_than_resolution_raises(self):
        with self.assertRaises(common.ParentResolutionException):
            h3(
                [
                    TEST_FILE_PATH,
                    str(TEST_OUTPUT_PATH),
                    "--layer",
                    TEST_LAYER_NAME,
                    "-r",
                    "8",
                    "-pr",
                    "8",
                ],
                standalone_mode=False,
            )

    def test_compact_without_id_field_raises(self):
        with self.assertRaises(common.IdFieldError):
            h3(
                [
                    TEST_FILE_PATH,
                    str(TEST_OUTPUT_PATH),
                    "--layer",
                    TEST_LAYER_NAME,
                    "-r",
                    "8",
                    "-co",
                ],
                standalone_mode=False,
            )

    def test_unknown_dggs_raises(self):
        with self.assertRaises(ValueError):
            indexer_instance("not_a_real_dggs")


class TestOverwriteRequired(TestRunthrough):
    """Requires a first successful run to create output, then checks the guard."""

    def test_overwrite_flag_required_on_second_run(self):
        h3(
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
        with self.assertRaises(FileExistsError):
            h3(
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
