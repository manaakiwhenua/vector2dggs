import multiprocessing
import warnings
import tempfile
from enum import StrEnum, unique

MIN_H3, MAX_H3 = 0, 15
MIN_RHP, MAX_RHP = 0, 15
MIN_S2, MAX_S2 = 0, 30
MIN_GEOHASH, MAX_GEOHASH = 1, 12
MIN_A5, MAX_A5 = 0, 30

# Assumed file descriptor soft limit (RLIMIT_NOFILE) on platforms where it
# cannot be queried (e.g. Windows, where the `resource` module is unavailable).
FALLBACK_RLIMIT_NOFILE = 1024


@unique
class SpatialSortingMethod(StrEnum):
    HILBERT = "hilbert"
    MORTON = "morton"
    GEOHASH = "geohash"
    NONE = "none"


SPATIAL_SORTING_METHODS = tuple(mode.value for mode in SpatialSortingMethod)


@unique
class GeoOutputMode(StrEnum):
    NONE = "none"
    POINT = "point"
    POLYGON = "polygon"


GEOM_TYPES = tuple(mode.value for mode in GeoOutputMode)

DEFAULT_DGGS_PARENT_RES = {
    "h3": lambda resolution: max(MIN_H3, (resolution - DEFAULT_PARENT_OFFSET)),
    "rhp": lambda resolution: max(MIN_RHP, (resolution - DEFAULT_PARENT_OFFSET)),
    "geohash": lambda resolution: max(
        MIN_GEOHASH, (resolution - DEFAULT_PARENT_OFFSET)
    ),
    "s2": lambda resolution: max(MIN_S2, (resolution - DEFAULT_PARENT_OFFSET)),
    "a5": lambda resolution: max(MIN_A5, (resolution - DEFAULT_PARENT_OFFSET)),
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

# https://github.com/felixpalmer/a5
A5_CELL_AREA_M2_BY_LEVEL = {
    0: 42505468731619.93,
    1: 8501093746323.985,
    2: 2125273436580.9963,
    3: 531318359145.2491,
    4: 132829589786.31227,
    5: 33207397446.578068,
    6: 8301849361.644517,
    7: 2075462340.4111292,
    8: 518865585.1027823,
    9: 129716396.27569558,
    10: 32429099.068923894,
    11: 8107274.767230974,
    12: 2026818.6918077434,
    13: 506704.67295193585,
    14: 126676.16823798396,
    15: 31669.04205949599,
    16: 7917.260514873998,
    17: 1979.3151287184994,
    18: 494.82878217962485,
    19: 123.70719554490621,
    20: 30.926798886226553,
    21: 7.731699721556638,
    22: 1.9329249303891596,
    23: 0.4832312325972899,
    24: 0.12080780814932247,
    25: 0.03020195203733062,
    26: 0.007550488009332655,
    27: 0.0018876220023331637,
    28: 0.0004719055005832909,
    29: 0.00011797637514582273,
    30: 2.9494093786455682e-05,
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
    "a5": lambda res: A5_CELL_AREA_M2_BY_LEVEL[res],
}

DEFAULT_AREA_THRESHOLD_M2 = lambda dggs, parent_res: DGGS_CELL_AREA_M2_BY_RES[dggs](
    parent_res
)

DEFAULTS = {
    "id": None,
    "k": False,
    "ch": 50,
    "s": SpatialSortingMethod.NONE.value,
    "crs": None,
    "c": None,
    "t": (multiprocessing.cpu_count() - 1),
    "cp": "snappy",
    "lyr": None,
    "g": "geom",
    "tempdir": tempfile.tempdir,
    "geo": GeoOutputMode.NONE.value,
}

warnings.filterwarnings(
    "ignore"
)  # This is to filter out the polyfill warnings when rows failed to get indexed at a resolution, can be commented out to find missing rows
