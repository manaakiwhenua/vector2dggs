import re
from collections.abc import Iterable, Sequence
from functools import cache
from itertools import product
from typing import cast

import geopandas as gpd
import pandas as pd
import shapely
from rhealpixdggs.conversion import compact_cells as rhp_compact_cells
from rhealpixdggs.dggs import WGS84_003, WGS84_003_OGC, RHEALPixDGGS
from rhealpixdggs.ellipsoids import WGS84_ELLIPSOID, Ellipsoid
from rhealpixdggs.rhp_wrappers import (
    linetrace,
    polyfill_array,
    rhp_get_resolution,
    rhp_to_center_child,
    rhp_to_geo,
    rhp_to_geo_boundary,
)
from shapely.geometry import LineString, Point, Polygon
from shapely.geometry.base import BaseGeometry

import vector2dggs.constants as const
from vector2dggs.indexers.vectorindexer import InvalidGeometryError, VectorIndexer

# rhealpixdggs's containment vocabulary (rhp_wrappers.polyfill_array)
_CONTAIN = {
    const.ContainmentMode.CENTRE: "center",
    const.ContainmentMode.INTERSECTS: "overlapping",
    const.ContainmentMode.WITHIN: "full",
}

# Cell side length by resolution (rHEALPix cells are equal-area squares, so
# side = sqrt(area)); reuses constants.py's table rather than a fresh library
# call per geometry.
_CELL_WIDTH_M = tuple(
    const.DGGS_CELL_AREA_M2_BY_RES["rhp"](res) ** 0.5
    for res in range(const.MIN_RHP, const.MAX_RHP + 1)
)

# rhealpixdggs calls here run under ProcessPoolExecutor workers or the main
# process only, never multiple threads in one process, so no lock is needed.

_GRIDS: dict[str, RHEALPixDGGS] = {
    "WGS84_003_OGC": WGS84_003_OGC,
    "WGS84_003": WGS84_003,
}


def _params(grid: RHEALPixDGGS) -> tuple[int, int, int, float]:
    return (
        grid.north_square,
        grid.south_square,
        grid.N_side,
        float(grid.ellipsoid.lon_0),
    )


@cache
def _resolve_grid(value: str | None) -> RHEALPixDGGS:
    """
    A preset by name, a registered DGGRS by URI, or a parameter spec
    (const.RHP_GRID_SPEC). A spec with a preset's parameters is that preset,
    so its codespace is kept. ValueError otherwise.
    """
    value = value or const.RHP_GRIDS[0]
    if value.startswith(("http://", "https://")):
        return RHEALPixDGGS.from_uri(value)
    if value in _GRIDS:
        return _GRIDS[value]
    spec = re.match(const.RHP_GRID_SPEC, value)
    if spec is None:
        raise ValueError(
            f"Unknown rHEALPix grid '{value}'. Options: {', '.join(const.RHP_GRIDS)}, "
            "a registered DGGRS URI, or <north><south><N_side>[@<lon_0>] such as 113"
        )
    north, south, n_side = (int(spec[k]) for k in ("north", "south", "n_side"))
    lon_0 = float(spec["lon_0"] or 0)
    if n_side != 3:
        raise ValueError(f"rHEALPix N_side {n_side} is not supported; only 3 is")
    for preset in _GRIDS.values():
        if _params(preset) == (north, south, n_side, lon_0):
            return preset
    return RHEALPixDGGS(
        ellipsoid=Ellipsoid(a=WGS84_ELLIPSOID.a, f=WGS84_ELLIPSOID.f, lon_0=lon_0),
        N_side=n_side,
        north_square=north,
        south_square=south,
    )


class RHPVectorIndexer(VectorIndexer[str]):
    """
    Provides integration for MWLR's rHEALPix DGGS.
    """

    GEODESIC_POLYFILL = False
    GRIDS = const.RHP_GRIDS

    def __init__(
        self,
        dggs: str,
        mode: str = const.ContainmentMode.CENTRE.value,
        grid: str | None = None,
    ):
        super().__init__(dggs, mode, grid)
        _resolve_grid(grid)  # reject an unknown grid now, not in a worker

    @property
    def grid(self) -> RHEALPixDGGS:
        # Resolved per process: a grid instance holds projection closures
        # that do not pickle, and the indexer is sent to pool workers.
        return _resolve_grid(self.grid_name)

    @staticmethod
    def _fill_min_res(geom: BaseGeometry, resolution: int) -> int:
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

    def _polyfill_polygon(self, geom: Polygon, resolution: int) -> list[str]:
        cells = polyfill_array(
            geom,
            resolution,
            plane=False,
            dggs=self.grid,
            containment=_CONTAIN[self.mode],
            min_res=RHPVectorIndexer._fill_min_res(geom, resolution),
        )
        # None, not an empty array, is rhealpixdggs refusing a malformed
        # geometry: one shapely holds invalid, or with no area. Said only
        # under verbose=True, so it would otherwise read as "no cells".
        # (An array's truthiness is ambiguous, unlike a set's - test for None.)
        if cells is None:
            raise InvalidGeometryError(
                f"rHEALPix rejects the polygon as {shapely.is_valid_reason(geom)}"
                if not geom.is_valid
                else "rHEALPix rejects the polygon as having no area"
            )
        return list(cells.tolist())

    def _linetrace(self, geom: LineString, resolution: int) -> list[str]:
        cells = linetrace(geom, resolution, plane=False, dggs=self.grid)
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
        cells = self.grid.cells_from_points(
            geom.x.to_numpy(), geom.y.to_numpy(), resolution, plane=False
        )
        result = df.drop(columns=[df.geometry.name])
        result.index = pd.Index(cells.astype(object))
        # empty string marks a point outside the planar image (no cell)
        return pd.DataFrame(result[result.index != ""].rename_axis(None))

    def cells_to_points(self, cells: Iterable[str]) -> Iterable[Point]:
        points = shapely.points(self.grid.centroids(list(cells), plane=False))
        return cast(Iterable[Point], points)

    def cells_to_polygons(self, cells: Iterable[str]) -> Iterable[Polygon]:
        polygons = shapely.polygons(
            self.grid.boundary_array(list(cells), n=2, plane=False)
        )
        return cast(Iterable[Polygon], polygons)

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
        col_order: list[str],
        dggs_col: str,
        id_field: str | Sequence[str],
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
        return set(rhp_compact_cells(cells))

    @staticmethod
    def get_resolution(cell: str) -> int:
        """
        Returns the resolution of a cell.

        Not a part of the interface provided by VectorIndexer.
        """
        return int(rhp_get_resolution(cell))

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

    def cell_to_point(self, cell: str) -> Point:
        return Point(rhp_to_geo(cell, plane=False, dggs=self.grid))

    def cell_to_polygon(self, cell: str) -> Polygon:
        return Polygon(
            tuple(
                coord
                for coord in rhp_to_geo_boundary(cell, plane=False, dggs=self.grid)
            )
        )
