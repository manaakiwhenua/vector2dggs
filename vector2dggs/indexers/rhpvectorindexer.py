from itertools import product

from rhealpixdggs.conversion import compress_order_cells
from rhealpixdggs.rhp_wrappers import (
    rhp_get_resolution,
    rhp_to_center_child,
    rhp_to_geo,
    rhp_to_geo_boundary,
)
from rhealpixdggs.dggs import WGS84_003
from rhppandas.util.const import COLUMNS

import rhppandas  # Necessary import despite lack of explicit use

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon

from vector2dggs.indexers.vectorindexer import VectorIndexer


class RHPVectorIndexer(VectorIndexer):
    """
    Provides integration for MWLR's rHEALPix DGGS.
    """

    def polyfill(self, df: gpd.GeoDataFrame, resolution: int) -> pd.DataFrame:
        geom_col = df.geometry.name
        parts = []

        df_polygon = df[df.geom_type == "Polygon"]
        if not df_polygon.empty:
            result = df_polygon.rhp.polyfill_resample(
                resolution, return_geometry=False, compress=False
            ).drop(columns=["index", geom_col])
            parts.append(pd.DataFrame(result))

        df_linestring = df[df.geom_type == "LineString"]
        if not df_linestring.empty:
            result = (
                df_linestring.rhp.linetrace(resolution)
                .explode(COLUMNS["linetrace"])
                .set_index(COLUMNS["linetrace"])
                .drop(columns=[geom_col])
            )
            result = result[~result.index.duplicated(keep="first")]
            parts.append(pd.DataFrame(result))

        df_point = df[df.geom_type == "Point"]
        if not df_point.empty:
            result = df_point.rhp.geo_to_rhp(resolution, set_index=True)
            parts.append(pd.DataFrame(result.drop(columns=[geom_col])))

        return pd.concat(parts) if parts else pd.DataFrame()

    def secondary_index(self, df: pd.DataFrame, parent_res: int) -> pd.DataFrame:
        """
        Implementation of abstract function.
        """

        return df.rhp.rhp_to_parent(parent_res)

    def compaction(
        self,
        df: pd.DataFrame,
        res: int,
        col_order: list,
        dggs_col: str,
        id_field: str,
        parent_res: int,
    ) -> pd.DataFrame:
        """
        Compacts an rHP dataframe up to a given low resolution (parent_res),
        from an existing maximum resolution (res).

        Implementation of abstract function.
        """
        return self.compaction_common(
            df,
            res,
            id_field,
            col_order,
            dggs_col,
            self.compact_cells,
            rhp_to_center_child,
            parent_res,
            self.get_resolution,
            self.children_at_res,
        )

    def compact_cells(self, cells: set[str]) -> set[str]:
        """
        Compact a set of rHEALPix DGGS cells.
        Cells must be at the same resolution.
        See https://github.com/manaakiwhenua/rhealpixdggs-py/issues/35#issuecomment-3186073554

        Not a part of the interface provided by VectorIndexer.
        """
        previous_result = set(cells)
        while True:
            current_result = set(compress_order_cells(previous_result))
            if previous_result == current_result:
                break
            previous_result = current_result
        return previous_result

    @staticmethod
    def get_resolution(cell: str) -> int:
        """
        Returns the resolution of a cell.

        Not a part of the interface provided by VectorIndexer.
        """
        return rhp_get_resolution(cell)

    @staticmethod
    def children_at_res(cell: str, target_res: int) -> list[str]:
        """
        Return all descendants of cell at resolution target_res.

        Not a part of the interface provided by VectorIndexer.
        """
        current_res = rhp_get_resolution(cell)
        if target_res <= current_res:
            return [cell]
        digits = "012345678"
        return [
            cell + "".join(suffix)
            for suffix in product(digits, repeat=target_res - current_res)
        ]

    @staticmethod
    def cell_to_point(cell: str) -> Point:
        return Point(rhp_to_geo(cell, plane=False, dggs=WGS84_003))

    @staticmethod
    def cell_to_polygon(cell: str) -> Polygon:
        return Polygon(
            tuple(
                coord
                for coord in rhp_to_geo_boundary(cell, plane=False, dggs=WGS84_003)
            )
        )
