from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent

TEST_FILE_PATH = str(DATA_DIR / "se-island.gpkg")
TEST_LAYER_NAME = "se_island"

TEST_LINESTRING_FILE_PATH = str(DATA_DIR / "se-island-contours.gpkg")
TEST_LINESTRING_LAYER_NAME = "contours"
TEST_POINT_FILE_PATH = str(DATA_DIR / "se-island-height-pts.gpkg")
TEST_POINT_LAYER_NAME = "nz_chatham_island_height_points_topo_150k"

TEST_ANTIMERIDIAN_FILE_PATH = str(DATA_DIR / "antimeridian.gpkg")
TEST_ANTIMERIDIAN_LAYER_NAME = "antimeridian_strip"

# 159 real MultiPolygon Z features, dispersed and partly overlapping (a mix
# of tiny and modest-sized parcels) - unlike the other fixtures above, this
# one exercises realistic real-world geometry complexity/scale variance.
TEST_DISPERSED_FILE_PATH = str(DATA_DIR / "chathams-pannz-2014.gpkg")
TEST_DISPERSED_LAYER_NAME = "chathams-pannz-2014"
