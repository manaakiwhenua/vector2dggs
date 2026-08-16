from unittest import SkipTest, TestCase

from vector2dggs.indexerfactory import indexer_instance

from ..data.datapaths import TEST_OUTPUT_PATH


def skip_unless_backend(dggs: str) -> None:
    """Skip the calling test/class when the backend's extra isn't installed."""
    try:
        indexer_instance(dggs)
    except ImportError as e:
        raise SkipTest(str(e)) from e


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
