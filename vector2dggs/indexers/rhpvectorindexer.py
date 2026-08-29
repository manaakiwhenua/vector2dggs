from collections.abc import Iterable

import geopandas as gpd
import pandas as pd
import rhealpixdggs as rh
from shapely.geometry import Point, Polygon

from vector2dggs.indexers.vectorindexer import VectorIndexer


class RHPVectorIndexer(VectorIndexer[str]):
    """
    Provides rHEALPix integration through the Rust-backed Python package.
    """

    GEODESIC_POLYFILL = False

    @staticmethod
    def _polyfill_polygon(geom, resolution: int) -> list[str]:
        return rh.geo.geometry_to_cells(geom, resolution)

    @staticmethod
    def _linetrace(geom, resolution: int) -> list[str]:
        coordinates = [(latitude, longitude) for longitude, latitude in geom.coords]
        # A traversal can re-enter a cell, but vector2dggs emits at most one
        # row per feature/cell pair.
        return list(dict.fromkeys(rh.line_to_cells(coordinates, resolution)))

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
            lambda geom, res: [rh.latlng_to_cell(geom.y, geom.x, res)],
            df.geometry.name,
        )

    def secondary_index(self, df: pd.DataFrame, parent_res: int) -> pd.DataFrame:
        """
        Implementation of abstract function.
        """

        df[f"rhp_{parent_res:02}"] = df.index.map(
            lambda cell: rh.cell_to_parent(cell, parent_res)
        )
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
            self.center_child_at_res,
            parent_res,
            self.get_resolution,
            self.children_at_res,
        )

    def compact_cells(self, cells: Iterable[str]) -> set[str]:
        """
        Compact a set of rHEALPix DGGS cells.
        Cells must be at the same resolution.
        See https://github.com/manaakiwhenua/rhealpixdggs-py/issues/35#issuecomment-3186073554

        Not a part of the interface provided by VectorIndexer.
        """
        return set(rh.compact_cells(list(cells)))

    @staticmethod
    def get_resolution(cell: str) -> int:
        """
        Returns the resolution of a cell.

        Not a part of the interface provided by VectorIndexer.
        """
        return rh.get_resolution(cell)

    @staticmethod
    def center_child_at_res(cell: str, target_res: int) -> str:
        """Return the aperture-9 centre descendant at ``target_res``."""
        current_res = rh.get_resolution(cell)
        if target_res <= current_res:
            return cell
        return cell + ("4" * (target_res - current_res))

    @staticmethod
    def children_at_res(cell: str, target_res: int) -> list[str]:
        """
        Return all descendants of cell at resolution target_res.

        Not a part of the interface provided by VectorIndexer.
        """
        current_res = rh.get_resolution(cell)
        if target_res <= current_res:
            return [cell]
        return rh.cell_to_children(cell, target_res)

    @staticmethod
    def cell_to_point(cell: str) -> Point:
        latitude, longitude = rh.cell_to_latlng(cell)
        return Point(longitude, latitude)

    @staticmethod
    def cell_to_polygon(cell: str) -> Polygon:
        return Polygon(
            tuple(
                (longitude, latitude)
                for latitude, longitude in rh.cell_to_boundary(cell)
            )
        )
