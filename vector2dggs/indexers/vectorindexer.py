from abc import ABC, abstractmethod
from uuid import uuid4
from typing import Union, Callable, Iterable

import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon, Point


class VectorIndexer(ABC):
    """
    Abstract base class and interface for all DGGS indexers.
    """

    def __init__(self, dggs: str):
        self.dggs = dggs

    @abstractmethod
    def polyfill(self, df: gpd.GeoDataFrame, resolution: int) -> pd.DataFrame: ...

    @abstractmethod
    def secondary_index(self, df: pd.DataFrame, parent_res: int) -> pd.DataFrame: ...

    @abstractmethod
    def compaction(
        self,
        df: pd.DataFrame,
        res: int,
        col_order: list,
        dggs_col: str,
        id_field: str,
    ) -> pd.DataFrame: ...

    @staticmethod
    @abstractmethod
    def cell_to_point(cell: str) -> Point: ...

    @staticmethod
    @abstractmethod
    def cell_to_polygon(cell: str) -> Polygon: ...

    @staticmethod
    def _geo_to_cells(
        df: gpd.GeoDataFrame, resolution: int, cell_fn, geom_col: str
    ) -> pd.DataFrame:
        return (
            df.assign(
                __cells__=df[geom_col].apply(lambda geom: cell_fn(geom, resolution))
            )
            .drop(columns=[geom_col])
            .explode("__cells__")
            .dropna(subset=["__cells__"])
            .set_index("__cells__")
            .rename_axis(None)
        )

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
        mask = pd.Series([False] * len(df), index=df.index)
        for key, value_set in uncompressable.items():
            mask |= (df[id_field] == key) & (df[dggs_col].isin(value_set))
        uncompressable_df = df[mask].set_index(dggs_col)

        # Get rows that can be compressed; replace fine cell with its compacted parent
        compression_mapping = {
            (id, cell_to_child_func(cell, res)): cell
            for id, cells in compressable.items()
            if cells
            for cell in cells
        }
        mask = pd.Series([False] * len(df), index=df.index)
        composite_key = f"composite_key_{uuid4()}"

        def get_composite_key(row):
            return (row[id_field], row[dggs_col])

        df[composite_key] = df.apply(get_composite_key, axis=1)
        mask |= df[composite_key].isin(compression_mapping)
        compressable_df = df[mask].copy()
        compressable_df[dggs_col] = compressable_df[composite_key].map(
            compression_mapping
        )
        compressable_df = compressable_df.set_index(dggs_col)

        return pd.concat([compressable_df, uncompressable_df])[col_order]
