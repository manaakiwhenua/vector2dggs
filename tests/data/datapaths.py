from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent

TEST_FILE_PATH = str(DATA_DIR / "se-island.gpkg")
TEST_LAYER_NAME = "se_island"
TEST_OUTPUT_PATH = DATA_DIR / "output"

TEST_LINESTRING_FILE_PATH = str(DATA_DIR / "se-island-contours.gpkg")
TEST_LINESTRING_LAYER_NAME = "contours"
TEST_POINT_FILE_PATH = str(DATA_DIR / "se-island-height-pts.gpkg")
TEST_POINT_LAYER_NAME = "nz_chatham_island_height_points_topo_150k"
