import multiprocessing
import warnings
import tempfile


MIN_H3, MAX_H3 = 0, 15
MIN_RHP, MAX_RHP = 0, 15
MIN_S2, MAX_S2 = 0, 30
MIN_GEOHASH, MAX_GEOHASH = 1, 12

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

# https://h3geo.org/docs/core-library/restable/
H3_CELLS_MAX_AREA_KM2_BY_LEVEL = {
    0: 4977807.027442012,
    1: 729486.875275344,
    2: 104599.807218925,
    3: 14950.773301379,
    4: 2135.986983965,
    5: 305.144308779,
    6: 43.592111685,
    7: 6.227445905,
    8: 0.889635157,
    9: 0.127090737,
    10: 0.018155820,
    11: 0.002593689,
    12: 0.000370527,
    13: 0.000052932,
    14: 0.000007562,
    15: 0.000001080,
}

RHP_CELLS_AREA_KM2_BY_LEVEL = {
    0: 100151150.62856922,
    1: 11127905.62539658,
    2: 1236433.9583773974,
    3: 137381.55093082195,
    4: 15264.616770091328,
    5: 1696.0685300101482,
    6: 188.4520588900164,
    7: 20.939117654446267,
    8: 2.326568628271808,
    9: 0.25850762536353417,
    10: 0.02872306948483713,
    11: 0.003191452164981904,
    12: 0.0003546057961091004,
    13: 3.940064401212227 * 1e-5,
    14: 4.377849334680252 * 1e-6,
    15: 4.864277038533613 * 1e-7,
}

# https://www.movable-type.co.uk/scripts/geohash.html
GEOHASH_MAX_CELL_AREA_KM2_BY_LEVEL = {
    1: 5000 * 5000,
    2: 1250 * 625,
    3: 156 * 156,
    4: 39.1 * 19.5,
    5: 4.89 * 4.89,
    6: 1.22 * 0.61,
    7: 153 * 153 / 1e6,
    8: 38.2 * 19.1 / 1e6,
    9: 4.77 * 4.77 / 1e6,
    10: 1.19 * 0.596 / 1e6,
    11: 149 * 149 / 1e9,
    12: 37.2 * 18.6 / 1e9,
}

DGGS_CELL_AREA_M2_BY_RES = {
    "s2": lambda res: S2_CELLS_MAX_AREA_M2_BY_LEVEL[res],
    "h3": lambda res: H3_CELLS_MAX_AREA_KM2_BY_LEVEL[res] * 1e6,
    "rhp": lambda res: RHP_CELLS_AREA_KM2_BY_LEVEL[res] * 1e6,
    "geohash": lambda res: GEOHASH_MAX_CELL_AREA_KM2_BY_LEVEL[res] * 1e6,
}

DEFAULT_AREA_THRESHOLD_M2 = lambda dggs, parent_res: DGGS_CELL_AREA_M2_BY_RES[dggs](
    parent_res
)

DEFAULTS = {
    "id": None,
    "k": False,
    "ch": 50,
    "s": "none",
    "crs": None,
    "c": None,
    "t": (multiprocessing.cpu_count() - 1),
    "cp": "snappy",
    "lyr": None,
    "g": "geom",
    "tempdir": tempfile.tempdir,
}


warnings.filterwarnings(
    "ignore"
)  # This is to filter out the polyfill warnings when rows failed to get indexed at a resolution, can be commented out to find missing rows
