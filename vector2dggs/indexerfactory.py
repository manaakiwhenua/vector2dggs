"""
@author: ndemaio, alpha-beta-soup
"""

from importlib import import_module
from typing import Dict, Tuple, Type

from vector2dggs.indexers import vectorindexer

INDEXER_LOOKUP: Dict[str, Tuple[str, str, str]] = {
    "h3": ("vector2dggs.indexers.h3vectorindexer", "H3VectorIndexer", "h3"),
    "rhp": ("vector2dggs.indexers.rhpvectorindexer", "RHPVectorIndexer", "rhp"),
    "geohash": (
        "vector2dggs.indexers.geohashvectorindexer",
        "GeohashVectorIndexer",
        "geohash",
    ),
    "s2": ("vector2dggs.indexers.s2vectorindexer", "S2VectorIndexer", "s2"),
    "a5": ("vector2dggs.indexers.a5vectorindexer", "A5VectorIndexer", "a5"),
}


def indexer_instance(dggs: str) -> vectorindexer.VectorIndexer:
    try:
        module_name, class_name, extra = INDEXER_LOOKUP[dggs]
    except KeyError as e:
        raise ValueError(
            f"Unknown DGGS: '{dggs}'. Options: {sorted(INDEXER_LOOKUP)}"
        ) from e

    try:
        module = import_module(module_name)
    except ModuleNotFoundError as e:
        raise ImportError(
            f"Mising dependency '{e.name}' for backend '{dggs}'.\n"
            f"Install optional dependencies: pip install 'vector2dggs[{extra}]' "
            f"(or 'vector2dggs[all]')."
        ) from e
    indexer: Type[vectorindexer.VectorIndexer] = getattr(module, class_name)
    return indexer(dggs)
