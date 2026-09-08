from collections.abc import Iterable
from itertools import product

import geopandas as gpd
import pandas as pd
import shapely
from rhealpixdggs.conversion import compact_cells as rhp_compact_cells
from rhealpixdggs.dggs import WGS84_003
from rhealpixdggs.rhp_wrappers import (
    linetrace,
    polyfill_array,
    rhp_get_resolution,
    rhp_to_center_child,
    rhp_to_geo,
    rhp_to_geo_boundary,
)
from shapely.geometry import Point, Polygon

import vector2dggs.constants as const
from vector2dggs.indexers.vectorindexer import VectorIndexer

# Cell side length by resolution (rHEALPix cells are equal-area squares, so
# side = sqrt(area)); reuses constants.py's table rather than a fresh library
# call per geometry.
_CELL_WIDTH_M = tuple(
    const.DGGS_CELL_AREA_M2_BY_RES["rhp"](res) ** 0.5
    for res in range(const.MIN_RHP, const.MAX_RHP + 1)
)

# rhealpixdggs calls here run under ProcessPoolExecutor workers or the main
# process only, never multiple threads in one process, so no lock is needed.


class RHPVectorIndexer(VectorIndexer[str]):
    """
    Provides integration for MWLR's rHEALPix DGGS.
    """

    GEODESIC_POLYFILL = False

    @staticmethod
    def _fill_min_res(geom, resolution: int) -> int:
        """
        Starting resolution for polyfill's hierarchical descent: the finest
        whose cell still covers geom's bbox. Affects speed, not output.

        Not a part of the interface provided by VectorIndexer.
        """
        minx, miny, maxx, maxy = geom.bounds
        if maxy >= 90 or miny <= -90 or maxx - minx >= 360:
            # pole-touching/antimeridian-spanning bbox: candidate region is
            # the whole polar square regardless of geom's real size
            return const.MIN_RHP
        span_m = max(maxx - minx, maxy - miny) * const.METRES_PER_DEGREE
        for res in range(min(resolution, const.MAX_RHP), const.MIN_RHP, -1):
            if _CELL_WIDTH_M[res] >= span_m:
                return res
        return const.MIN_RHP

    @staticmethod
    def _polyfill_polygon(geom, resolution: int) -> list:
        cells = polyfill_array(
            geom,
            resolution,
            plane=False,
            dggs=WGS84_003,
            min_res=RHPVectorIndexer._fill_min_res(geom, resolution),
        )
        # an array's truthiness is ambiguous, unlike a set's - test for None
        return [] if cells is None else cells.tolist()

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
        geom = df[df.geometry.name]
        cells = WGS84_003.cells_from_points(
            geom.x.to_numpy(), geom.y.to_numpy(), resolution, plane=False
        )
        result = df.drop(columns=[df.geometry.name])
        result.index = pd.Index(cells.astype(object))
        # empty string marks a point outside the planar image (no cell)
        return pd.DataFrame(result[result.index != ""].rename_axis(None))

    def cells_to_points(self, cells: Iterable[str]) -> Iterable[Point]:
        return shapely.points(WGS84_003.centroids(list(cells), plane=False))

    def cells_to_polygons(self, cells: Iterable[str]) -> Iterable[Polygon]:
        return shapely.polygons(WGS84_003.boundary_array(list(cells), n=2, plane=False))

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
