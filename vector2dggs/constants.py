import multiprocessing
import warnings
import tempfile


MIN_H3, MAX_H3 = 0, 15
MIN_RHP, MAX_RHP = 0, 15

DEFAULTS = {
    "id": None,
    "k": False,
    "ch": 50,
    "s": "hilbert",
    "crs": None,
    "c": 5000,
    "t": (multiprocessing.cpu_count() - 1),
    "tbl": None,
    "g": "geom",
    "tempdir": tempfile.tempdir,
}

DEFAULT_PARENT_OFFSET = 6

warnings.filterwarnings(
    "ignore"
)  # This is to filter out the polyfill warnings when rows failed to get indexed at a resolution, can be commented out to find missing rows
