from unittest import *
from data.datapaths import *


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
