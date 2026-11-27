"""
Every DGGS command, from one table: a plain Click command each (`h3`, `rhp`,
..., `isea9r`), built by cli_factory.make_dggs_command with that backend's
own options. cli.py registers COMMANDS; tests import the commands by name.
"""

from typing import Any

import vector2dggs.constants as const
from vector2dggs.cli_factory import make_dggs_command

_SPECS: tuple[tuple[str, str, int, int, dict[str, Any]], ...] = (
    ("h3", "H3", const.MIN_H3, const.MAX_H3, {"int_cells": True}),
    (
        "rhp",
        "rHEALPix",
        const.MIN_RHP,
        const.MAX_RHP,
        {
            "int_cells": True,
            "grid_default": const.RHP_DEFAULT_GRID,
            "res_note": " (up to 15 for an N_side 3 grid, the default; 24 for N_side 2: see --grid)",
        },
    ),
    ("s2", "S2", const.MIN_S2, const.MAX_S2, {"int_cells": True}),
    (
        "a5",
        "A5",
        const.MIN_A5,
        const.MAX_A5,
        {
            "int_cells": True,
            # pya5 has no wholly-within test; see A5VectorIndexer.SUPPORTED_MODES
            "modes": (
                const.ContainmentMode.CENTRE.value,
                const.ContainmentMode.INTERSECTS.value,
            ),
        },
    ),
    ("geohash", "Geohash", const.MIN_GEOHASH, const.MAX_GEOHASH, {}),
    # DGGAL grids
    ("isea4r", "ISEA4R", 0, const.MAX_ISEA4R, {"int_cells": True}),
    ("ivea4r", "IVEA4R", 0, const.MAX_IVEA4R, {"int_cells": True}),
    ("rtea4r", "RTEA4R", 0, const.MAX_RTEA4R, {"int_cells": True}),
    ("isea9r", "ISEA9R", 0, const.MAX_ISEA9R, {"int_cells": True}),
    ("ivea9r", "IVEA9R", 0, const.MAX_IVEA9R, {"int_cells": True}),
    ("rtea9r", "RTEA9R", 0, const.MAX_RTEA9R, {"int_cells": True}),
    ("healpix", "HEALPix", 0, const.MAX_HEALPIX, {"int_cells": True}),
    ("healpix_nuniq", "HEALPix NUNIQ", 0, const.MAX_HEALPIX_NUNIQ, {"int_cells": True}),
)

(
    h3,
    rhp,
    s2,
    a5,
    geohash,
    isea4r,
    ivea4r,
    rtea4r,
    isea9r,
    ivea9r,
    rtea9r,
    healpix,
    healpix_nuniq,
) = (
    make_dggs_command(key, key, display, min_res, max_res, **options)
    for key, display, min_res, max_res, options in _SPECS
)

COMMANDS = (
    h3,
    rhp,
    s2,
    a5,
    geohash,
    isea4r,
    ivea4r,
    rtea4r,
    isea9r,
    ivea9r,
    rtea9r,
    healpix,
    healpix_nuniq,
)
