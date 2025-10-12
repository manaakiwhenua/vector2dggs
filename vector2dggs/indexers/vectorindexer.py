"""

@author: ndemaio
"""

from uuid import uuid4
from typing import Union, Callable, Iterable

import pandas as pd
import geopandas as gpd


class VectorIndexer:
    """
    Provides an abstract base class and interface for all indexers integrating
    a specific DGGS. It should never be instantiated directly because some
    methods raise a NotImplementedError by design. Those methods should be
    implemented by the child classes deriving from this interface instead.
    """
    
    def __init__(self, dggs: str):
        """
        Value used across all child classes
        """
        self.dggs = dggs
        
    def polyfill(self, df: gpd.GeoDataFrame, resolution: int) -> pd.DataFrame:
        """
        Needs to be implemented by child class
        """
        raise NotImplementedError()
        
    def secondary_index(self, df: pd.DataFrame, parent_res: int) -> pd.DataFrame:
        """
        Needs to be implemented by child class
        """
        raise NotImplementedError()
        
    def compaction(
        self,
        df: pd.DataFrame,
        res: int,
        col_order: list,
        dggs_col: str,
        id_field: str,
    ) -> pd.DataFrame:
        """
        Needs to be implemented by child class
        """
        raise NotImplementedError()
        
    def compaction_common(
        self,
        df: pd.DataFrame,
        res: int,
        id_field: str,
        col_order: list[str],
        dggs_col: str,
        compact_func: Callable[[Iterable[Union[str, int]]], Iterable[Union[str, int]]],
        cell_to_child_func: Callable[[Union[str, int], int], Union[str, int]],
    ):
        """
        Compacts a dataframe up to a given low resolution (parent_res), from an existing maximum resolution (res).
        """
        df = df.reset_index(drop=False)

        feature_cell_groups = (
            df.groupby(id_field)[dggs_col].apply(lambda x: set(x)).to_dict()
        )
        feature_cell_compact = {
            id: set(compact_func(cells)) for id, cells in feature_cell_groups.items()
        }

        uncompressable = {
            id: feature_cell_groups[id] & feature_cell_compact[id]
            for id in feature_cell_groups.keys()
        }
        compressable = {
            id: feature_cell_compact[id] - feature_cell_groups[id]
            for id in feature_cell_groups.keys()
        }

        # Get rows that cannot be compressed
        mask = pd.Series([False] * len(df), index=df.index)  # Init bool mask
        for key, value_set in uncompressable.items():
            mask |= (df[id_field] == key) & (df[dggs_col].isin(value_set))
        uncompressable_df = df[mask].set_index(dggs_col)

        # Get rows that can be compressed
        # Convert each compressed (coarser resolution) cell into a cell at
        #   the original resolution (usu using centre child as reference)
        compression_mapping = {
            (id, cell_to_child_func(cell, res)): cell
            for id, cells in compressable.items()
            if cells
            for cell in cells
        }
        mask = pd.Series([False] * len(df), index=df.index)
        composite_key = f"composite_key_{uuid4()}"
        # Update mask for compressible rows and prepare for replacement
        get_composite_key = lambda row: (row[id_field], row[dggs_col])
        df[composite_key] = df.apply(get_composite_key, axis=1)
        mask |= df[composite_key].isin(compression_mapping)
        compressable_df = df[mask].copy()
        compressable_df[dggs_col] = compressable_df[composite_key].map(
            compression_mapping
        )  # Replace DGGS cell ID with compressed representation
        compressable_df = compressable_df.set_index(dggs_col)

        return pd.concat([compressable_df, uncompressable_df])[col_order]

