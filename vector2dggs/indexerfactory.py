"""

@author: ndemaio
"""

from vector2dggs.indexers import vectorindexer

import vector2dggs.indexers.h3vectorindexer as h3vectorindexer
import vector2dggs.indexers.rhpvectorindexer as rhpvectorindexer
import vector2dggs.indexers.geohashvectorindexer as geohashvectorindexer
import vector2dggs.indexers.s2vectorindexer as s2vectorindexer


"""
Match DGGS name to indexer class name
"""
indexer_lookup = {
    "h3": h3vectorindexer.H3VectorIndexer,
    "rhp": rhpvectorindexer.RHPVectorIndexer,
    "geohash": geohashvectorindexer.GeohashVectorIndexer,
    "s2": s2vectorindexer.S2VectorIndexer,
}


"""
Looks up and instantiates an appropriate indexer class given a DGGS name
as defined in the list of click commands
"""


def indexer_instance(dggs: str) -> vectorindexer.VectorIndexer:
    # Create and return appropriate indexer instance
    # Raise an exception for unsupported DGGS names
    if not dggs in indexer_lookup.keys():
        raise RuntimeError(
            "Unknown dggs {dggs}) -  must be one of [ {options} ]".format(
                dggs=dggs, options=", ".join(indexer_lookup.keys())
            )
        )

    indexer = indexer_lookup[dggs]
    return indexer(dggs)
