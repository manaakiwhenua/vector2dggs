from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import Point, Polygon


class VectorIndexer(ABC):
    """
    Abstract base class and interface for all DGGS indexers.
    """

    # Whether this backend's polyfill does its point-in-polygon containment
    # test geodesically (on the sphere) rather than on planar coordinates.
    # A geodesic polyfill indexes antimeridian-crossing geometries correctly
    # whether or not they've been pre-split; a planar one does not.
    GEODESIC_POLYFILL: bool = False

    def __init__(self, dggs: str):
        self.dggs = dggs

    def polyfill(self, df: gpd.GeoDataFrame, resolution: int) -> pd.DataFrame:
        """
        Splits df by geometry type, dispatches each non-empty subset to the
        corresponding _polyfill_* implementation, and concatenates the results.
        """
        parts = []

        df_polygon = df[df.geom_type == "Polygon"]
        if not df_polygon.empty:
            parts.append(self._polyfill_polygons(df_polygon, resolution))

        df_linestring = df[df.geom_type == "LineString"]
        if not df_linestring.empty:
            parts.append(self._polyfill_linestrings(df_linestring, resolution))

        df_point = df[df.geom_type == "Point"]
        if not df_point.empty:
            parts.append(self._polyfill_points(df_point, resolution))

        return pd.concat(parts) if parts else pd.DataFrame()

    @abstractmethod
    def _polyfill_polygons(
        self, df: gpd.GeoDataFrame, resolution: int
    ) -> pd.DataFrame: ...

    @abstractmethod
    def _polyfill_linestrings(
        self, df: gpd.GeoDataFrame, resolution: int
    ) -> pd.DataFrame: ...

    @abstractmethod
    def _polyfill_points(
        self, df: gpd.GeoDataFrame, resolution: int
    ) -> pd.DataFrame: ...

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
        parent_res: int,
    ) -> pd.DataFrame: ...

    @staticmethod
    @abstractmethod
    def cell_to_point(cell: str) -> Point: ...

    @staticmethod
    @abstractmethod
    def cell_to_polygon(cell: str) -> Polygon: ...

    @staticmethod
    @abstractmethod
    def get_resolution(cell: str) -> int: ...

    @staticmethod
    @abstractmethod
    def children_at_res(cell: str, target_res: int) -> Iterable[str]: ...

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

    @staticmethod
    def _enforce_resolution_floor(
        cells: Iterable[str],
        parent_res: int,
        get_resolution_func: Callable[[str], int],
        children_at_res_func: Callable[[str, int], Iterable[str]],
    ) -> set[str]:
        """
        Break up any cell coarser than parent_res into its children at
        parent_res, so that no cell in the result is coarser than parent_res.
        """
        result: set[str] = set()
        for cell in cells:
            if get_resolution_func(cell) < parent_res:
                result.update(children_at_res_func(cell, parent_res))
            else:
                result.add(cell)
        return result

    def compaction_common(
        self,
        df: pd.DataFrame,
        res: int,
        id_field: str,
        col_order: list[str],
        dggs_col: str,
        compact_func: Callable[[Iterable[str]], Iterable[str]],
        cell_to_child_func: Callable[[str, int], str],
        parent_res: int,
        get_resolution_func: Callable[[str], int],
        children_at_res_func: Callable[[str, int], Iterable[str]],
    ):
        """
        Compacts a dataframe up to a given low resolution (parent_res), from an existing maximum resolution (res).

        Any cell coarser than parent_res produced by compact_func is broken
        back up into its children at parent_res, so the result never contains
        a cell coarser than parent_res.
        """
        df = df.reset_index(drop=False)
        # Captured before any construction below, so a fresh pd.Series built
        # from a plain Python list of cell IDs (e.g. parent_for_pair below)
        # can be pinned to it explicitly. Left to inference, pandas picks
        # int64 or uint64 per list based on whether any value exceeds
        # 2**63-1 - inconsistently with the source column, whose dtype this
        # sidesteps entirely - and concatenating mismatched int64/uint64
        # frames silently upcasts the result to float64, corrupting cell IDs.
        cell_dtype = df[dggs_col].dtype

        if df.empty:
            # e.g. an empty partition after a shuffle; the mask logic below
            # misbehaves on empty frames (object-dtype mask)
            return df.set_index(dggs_col)[col_order]

        feature_cell_groups = (
            df.groupby(id_field)[dggs_col].apply(lambda x: set(x)).to_dict()
        )
        feature_cell_compact = {
            id: set(compact_func(cells)) for id, cells in feature_cell_groups.items()
        }

        feature_cell_compact = {
            id: self._enforce_resolution_floor(
                cells, parent_res, get_resolution_func, children_at_res_func
            )
            for id, cells in feature_cell_compact.items()
        }

        uncompressable = {
            id: feature_cell_groups[id] & feature_cell_compact[id]
            for id in feature_cell_groups
        }
        compressable = {
            id: feature_cell_compact[id] - feature_cell_groups[id]
            for id in feature_cell_groups
        }

        pairs = pd.MultiIndex.from_arrays([df[id_field], df[dggs_col]])

        # Rows kept as-is: their (id, cell) survived compaction unchanged
        keep = [(id, cell) for id, cells in uncompressable.items() for cell in cells]
        keep_mask = pairs.isin(keep) if keep else np.zeros(len(df), dtype=bool)
        uncompressable_df = df[keep_mask].set_index(dggs_col)

        # Rows compressed: one representative child row per compacted cell,
        # with the fine cell replaced by its compacted parent
        compression_mapping = {
            (id, cell_to_child_func(cell, res)): cell
            for id, cells in compressable.items()
            if cells
            for cell in cells
        }
        if compression_mapping:
            parent_for_pair = pd.Series(
                list(compression_mapping.values()),
                index=pd.MultiIndex.from_tuples(list(compression_mapping.keys())),
                dtype=cell_dtype,
            )
            sel = pairs.isin(parent_for_pair.index)
            compressable_df = (
                df[sel]
                .assign(**{dggs_col: parent_for_pair.reindex(pairs[sel]).to_numpy()})
                .set_index(dggs_col)
            )
        else:
            compressable_df = df.iloc[0:0].set_index(dggs_col)

        return pd.concat([compressable_df, uncompressable_df])[col_order]
