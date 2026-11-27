import re
from collections.abc import Iterable, Sequence
from functools import cache
from itertools import product
from typing import Any, cast

import geopandas as gpd
import pandas as pd
import shapely
from rhealpixdggs.conversion import compact_cells as rhp_compact_cells
from rhealpixdggs.dggs import WGS84_003_OGC, RHEALPixDGGS
from rhealpixdggs.ellipsoids import WGS84_ELLIPSOID, Ellipsoid
from rhealpixdggs.rhp_wrappers import (
    linetrace,
    polyfill_array,
    rhp_get_resolution,
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


@cache
def _cell_widths_m(grid_name: str | None) -> tuple[float, ...]:
    """Cell side by resolution: equal-area squares, so side = sqrt(area)."""
    grid = _resolve_grid(grid_name)
    return tuple(
        float(grid.cell_area(res, plane=False)) ** 0.5
        for res in range(const.MIN_RHP, grid.max_resolution + 1)
    )


# rhealpixdggs calls here run under ProcessPoolExecutor workers or the main
# process only, never multiple threads in one process, so no lock is needed.

# Registered grids, so a spec with their parameters keeps their codespace.
_REGISTERED = (WGS84_003_OGC,)


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
    A registered DGGRS by URI, or a WGS84 grid from a parameter spec
    (const.RHP_GRID_SPEC). A spec with a registered grid's parameters is
    that grid, so its codespace is kept. ValueError otherwise.
    """
    value = value or const.RHP_DEFAULT_GRID
    if value.startswith(("http://", "https://")):
        return _canonical(RHEALPixDGGS.from_uri(value))
    spec = re.match(const.RHP_GRID_SPEC, value)
    if spec is None:
        raise ValueError(
            f"Unknown rHEALPix grid '{value}': neither a registered DGGRS URI "
            "nor <north><south><N_side>[@<lon_0>] (N_side 2 or 3) such as 003 "
            "or 123@50"
        )
    north, south, n_side = (int(spec[k]) for k in ("north", "south", "n_side"))
    lon_0 = float(spec["lon_0"] or 0)
    return _canonical(
        RHEALPixDGGS(
            ellipsoid=Ellipsoid(a=WGS84_ELLIPSOID.a, f=WGS84_ELLIPSOID.f, lon_0=lon_0),
            N_side=n_side,
            north_square=north,
            south_square=south,
        )
    )


def _canonical(grid: RHEALPixDGGS) -> RHEALPixDGGS:
    """The registered object for a grid with a registered grid's parameters."""
    for registered in _REGISTERED:
        if _params(registered) == _params(grid):
            return registered
    return grid


class RHPVectorIndexer(VectorIndexer[str]):
    """
    Provides integration for MWLR's rHEALPix DGGS.
    """

    GEODESIC_POLYFILL = False
    GRID_DEFAULT = const.RHP_DEFAULT_GRID
    LIBRARIES = ("rhealpixdggs",)

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

    def grid_metadata(self) -> dict[str, Any]:
        # the DGGRS definition in OGC API - DGGS's schema: what the codespace
        # URI resolves to, or a custom grid's full description
        return {
            "codespace": self.grid.codespace,
            "grid": self.grid_name or const.RHP_DEFAULT_GRID,
            "definition": self.grid.to_dggrs_definition(),
        }

    def cell_area_m2(self, resolution: int) -> float:
        return float(self.grid.cell_area(resolution, plane=False))

    @property
    def max_res(self) -> int:
        return int(self.grid.max_resolution)

    def _fill_min_res(self, geom: BaseGeometry, resolution: int) -> int:
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
        widths = _cell_widths_m(self.grid_name)
        for res in range(min(resolution, self.max_res), const.MIN_RHP, -1):
            if widths[res] >= span_m:
                return res
        return const.MIN_RHP

    def _polyfill_polygon(self, geom: Polygon, resolution: int) -> list[str]:
        cells = polyfill_array(
            geom,
            resolution,
            plane=False,
            dggs=self.grid,
            containment=_CONTAIN[self.mode],
            min_res=self._fill_min_res(geom, resolution),
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
        # any descendant serves as the representative row; a centre child
        # only exists for an odd N_side
        return self.compaction_common(
            df,
            res,
            id_field,
            col_order,
            dggs_col,
            self.compact_cells,
            lambda cell, target: self.children_at_res(cell, target)[0],
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
        return set(rhp_compact_cells(cells, N_side=self.grid.N_side))

    @staticmethod
    def get_resolution(cell: str) -> int:
        """
        Returns the resolution of a cell.

        Not a part of the interface provided by VectorIndexer.
        """
        return int(rhp_get_resolution(cell))

    def children_at_res(self, cell: str, target_res: int) -> list[str]:
        """
        Return all descendants of cell at resolution target_res.

        Not a part of the interface provided by VectorIndexer.
        """
        current_res = rhp_get_resolution(cell)
        if target_res <= current_res:
            return [cell]
        digits = "".join(str(d) for d in range(self.grid.N_side**2))
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
