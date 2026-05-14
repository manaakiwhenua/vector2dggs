from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent

TEST_FILE_PATH = str(DATA_DIR / "se-island.gpkg")
TEST_LAYER_NAME = "se_island"
TEST_OUTPUT_PATH = DATA_DIR / "output"
