"""

@author: ndemaio
"""

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