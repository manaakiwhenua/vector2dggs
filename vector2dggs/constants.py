import multiprocessing
import warnings
import tempfile


MIN_H3, MAX_H3 = 0, 15
MIN_RHP, MAX_RHP = 0, 15
MIN_S2, MAX_S2 = 0, 30
MIN_GEOHASH, MAX_GEOHASH = 1, 12

DEFAULTS = {
    "id": None,
    "k": False,
    "ch": 50,
    "s": "none",
    "crs": None,
    "c": 5000,
    "t": (multiprocessing.cpu_count() - 1),
    "cp": "snappy",
    "lyr": None,
    "g": "geom",
    "tempdir": tempfile.tempdir,
}

SPATIAL_SORTING_METHODS = ["hilbert", "morton", "geohash", "none"]

DEFAULT_DGGS_PARENT_RES = {
    "h3": lambda resolution: max(MIN_H3, (resolution - DEFAULT_PARENT_OFFSET)),
    "rhp": lambda resolution: max(MIN_RHP, (resolution - DEFAULT_PARENT_OFFSET)),
    "geohash": lambda resolution: max(
        MIN_GEOHASH, (resolution - DEFAULT_PARENT_OFFSET)
    ),
    "s2": lambda resolution: max(MIN_S2, (resolution - DEFAULT_PARENT_OFFSET)),
}

DEFAULT_PARENT_OFFSET = 6

# http://s2geometry.io/resources/s2cell_statistics.html
S2_CELLS_MAX_AREA_M2_BY_LEVEL = {
    0: 85011012.19 * 1e6,
    1: 21252753.05 * 1e6,
    2: 6026521.16 * 1e6,
    3: 1646455.50 * 1e6,
    4: 413918.15 * 1e6,
    5: 104297.91 * 1e6,
    6: 26113.30 * 1e6,
    7: 6529.09 * 1e6,
    8: 1632.45 * 1e6,
    9: 408.12 * 1e6,
    10: 102.03 * 1e6,
    11: 25.51 * 1e6,
    12: 6.38 * 1e6,
    13: 1.59 * 1e6,
    14: 0.40 * 1e6,
    15: 99638.93,
    16: 24909.73,
    17: 6227.43,
    18: 1556.86,
    19: 389.22,
    20: 97.30,
    21: 24.33,
    22: 6.08,
    23: 1.52,
    24: 0.38,
    25: 950.23 * 1e-4,
    26: 237.56 * 1e-4,
    27: 59.39 * 1e-4,
    28: 14.85 * 1e-4,
    29: 3.71 * 1e-4,
    30: 0.93 * 1e-4,
}


warnings.filterwarnings(
    "ignore"
)  # This is to filter out the polyfill warnings when rows failed to get indexed at a resolution, can be commented out to find missing rows
