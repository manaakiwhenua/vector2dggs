"""
@author: ndemaio
"""

from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent

TEST_FILE_PATH = str(DATA_DIR / "se-island.gpkg")
TEST_OUTPUT_PATH = DATA_DIR / "output"
