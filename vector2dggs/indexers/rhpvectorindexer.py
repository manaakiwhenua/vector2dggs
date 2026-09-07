from collections.abc import Iterable
from itertools import product

import geopandas as gpd
import pandas as pd
from rhealpixdggs.dggs import WGS84_003
from rhealpixdggs.rhp_wrappers import (
    compact_cells as rhp_compact_cells,
)
from rhealpixdggs.rhp_wrappers import (
    geo_to_rhp,
    linetrace,
    polyfill,
    rhp_get_resolution,
    rhp_to_center_child,
    rhp_to_geo,
    rhp_to_geo_boundary,
)
from shapely.geometry import Point, Polygon

from vector2dggs.indexers.vectorindexer import VectorIndexer


class RHPVectorIndexer(VectorIndexer[str]):
    """
    Provides integration for MWLR's rHEALPix DGGS.
    """

    GEODESIC_POLYFILL = False

    @staticmethod
    def _polyfill_polygon(geom, resolution: int) -> list:
        cells = polyfill(geom, resolution, plane=False, dggs=WGS84_003)
        return list(cells) if cells else []

    @staticmethod
    def _linetrace(geom, resolution: int) -> list:
        cells = linetrace(geom, resolution, plane=False, dggs=WGS84_003)
        # linetrace returns a traversal sequence, which may revisit cells
        return list(dict.fromkeys(cells)) if cells else []

    def _polyfill_polygons(self, df: gpd.GeoDataFrame, resolution: int) -> pd.DataFrame:
        return self._geo_to_cells(
            df, resolution, self._polyfill_polygon, df.geometry.name
        )

    def _polyfill_linestrings(
        self, df: gpd.GeoDataFrame, resolution: int
    ) -> pd.DataFrame:
        return self._geo_to_cells(df, resolution, self._linetrace, df.geometry.name)

    def _polyfill_points(self, df: gpd.GeoDataFrame, resolution: int) -> pd.DataFrame:
        return self._geo_to_cells(
            df,
            resolution,
            lambda geom, res: [geo_to_rhp(geom.y, geom.x, res, plane=False)],
            df.geometry.name,
        )

    def secondary_index(self, df: pd.DataFrame, parent_res: int) -> pd.DataFrame:
        """
        Implementation of abstract function.

        A cell's ancestor at parent_res is its address prefix (the rHEALPix
        addressing convention), so this is a vectorised string slice rather
        than a per-cell library call.
        """
        df[f"rhp_{parent_res:02}"] = df.index.str[: parent_res + 1]
        return df

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

    def compact_cells(self, cells: Iterable[str]) -> set[str]:
        """
        Compact a set of rHEALPix DGGS cells.
        Cells must be at the same resolution.

        Not a part of the interface provided by VectorIndexer.
        """
        return rhp_compact_cells(cells)

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
