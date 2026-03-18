"""
@author: ndemaio
"""

import unittest
from pathlib import Path

if __name__ == "__main__":
    tests_dir = Path(__file__).resolve().parent
    top_level_dir = tests_dir.parent
    testSuite = unittest.defaultTestLoader.discover(
        start_dir=str(tests_dir),
        top_level_dir=str(top_level_dir),
    )
    testRunner = unittest.TextTestRunner()
    testRunner.run(testSuite)
