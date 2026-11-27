from importlib import import_module
from typing import Any

import vector2dggs.constants as const
from vector2dggs.indexers import vectorindexer

INDEXER_LOOKUP: dict[str, tuple[str, str]] = {
    "h3": ("vector2dggs.indexers.h3vectorindexer", "H3VectorIndexer"),
    "rhp": ("vector2dggs.indexers.rhpvectorindexer", "RHPVectorIndexer"),
    "geohash": ("vector2dggs.indexers.geohashvectorindexer", "GeohashVectorIndexer"),
    "s2": ("vector2dggs.indexers.s2vectorindexer", "S2VectorIndexer"),
    "a5": ("vector2dggs.indexers.a5vectorindexer", "A5VectorIndexer"),
    "isea4r": ("vector2dggs.indexers.dggalvectorindexer", "ISEA4RVectorIndexer"),
    "ivea4r": ("vector2dggs.indexers.dggalvectorindexer", "IVEA4RVectorIndexer"),
    "rtea4r": ("vector2dggs.indexers.dggalvectorindexer", "RTEA4RVectorIndexer"),
    "isea9r": ("vector2dggs.indexers.dggalvectorindexer", "ISEA9RVectorIndexer"),
    "ivea9r": ("vector2dggs.indexers.dggalvectorindexer", "IVEA9RVectorIndexer"),
    "rtea9r": ("vector2dggs.indexers.dggalvectorindexer", "RTEA9RVectorIndexer"),
    "healpix": ("vector2dggs.indexers.dggalvectorindexer", "HEALPixVectorIndexer"),
    "healpix_nuniq": (
        "vector2dggs.indexers.dggalvectorindexer",
        "HEALPixNUNIQVectorIndexer",
    ),
}


def indexer_instance(
    dggs: str,
    mode: str = const.ContainmentMode.CENTRE.value,
    grid: str | None = None,
) -> vectorindexer.VectorIndexer[Any]:
    try:
        module_name, class_name = INDEXER_LOOKUP[dggs]
    except KeyError as e:
        raise ValueError(
            f"Unknown DGGS: '{dggs}'. Options: {sorted(INDEXER_LOOKUP)}"
        ) from e

    try:
        module = import_module(module_name)
    except ModuleNotFoundError as e:
        raise ImportError(
            f"Missing dependency '{e.name}' for backend '{dggs}'.\n"
            f"Install optional dependencies: pip install 'vector2dggs[{dggs}]' "
            f"(or 'vector2dggs[all]')."
        ) from e
    indexer: type[vectorindexer.VectorIndexer[Any]] = getattr(module, class_name)
    return indexer(dggs, mode, grid)
