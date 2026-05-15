from importlib import import_module
from typing import Dict, Tuple, Type

from vector2dggs.indexers import vectorindexer

INDEXER_LOOKUP: Dict[str, Tuple[str, str]] = {
    "h3": ("vector2dggs.indexers.h3vectorindexer", "H3VectorIndexer"),
    "rhp": ("vector2dggs.indexers.rhpvectorindexer", "RHPVectorIndexer"),
    "geohash": ("vector2dggs.indexers.geohashvectorindexer", "GeohashVectorIndexer"),
    "s2": ("vector2dggs.indexers.s2vectorindexer", "S2VectorIndexer"),
    "a5": ("vector2dggs.indexers.a5vectorindexer", "A5VectorIndexer"),
}


def indexer_instance(dggs: str) -> vectorindexer.VectorIndexer:
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
    indexer: Type[vectorindexer.VectorIndexer] = getattr(module, class_name)
    return indexer(dggs)
