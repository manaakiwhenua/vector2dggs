from collections.abc import Iterable

import geopandas as gpd
import h3
import numpy as np
import pandas as pd
import pyarrow as pa
import shapely
from h3.api import basic_int
from shapely.geometry import Point, Polygon, mapping

import vector2dggs.constants as const
from vector2dggs.indexers.vectorindexer import VectorIndexer

# h3-py's containment vocabulary. Only h3shape_to_cells_experimental offers
# these, and upstream declares that API unstable, so centre mode stays on
# the stable geo_to_cells.
_CONTAIN = {
    const.ContainmentMode.INTERSECTS: "overlap",
    const.ContainmentMode.WITHIN: "full",
}


def _as_int(cell: str | int) -> int:
    """
    Accepts either form: the working native int (from within the
    pipeline), or the h3 string token (e.g. reading a --cell-id string
    output file back and using these methods as a standalone convenience,
    the way tests do against the pipeline's own default output).
    """
    return h3.str_to_int(cell) if isinstance(cell, str) else int(cell)


class H3VectorIndexer(VectorIndexer[str | int]):
    """
    Provides integration for Uber's H3 DGGS.

    h3-py cells are natively int (h3.api.basic_int); the string token form
    is produced only at the output boundary via cells_to_string.
    """

    GEODESIC_POLYFILL = True
    CELL_ARROW_TYPE: pa.DataType = pa.uint64()

    @staticmethod
    def cells_to_string(cells: Iterable[str | int]) -> list[str]:
        return [h3.int_to_str(int(c)) for c in cells]

    def _polyfill_polygon(self, geom, resolution: int) -> list:
        if self.mode == const.ContainmentMode.CENTRE:
            return basic_int.geo_to_cells(mapping(geom), resolution)
        return basic_int.h3shape_to_cells_experimental(
            basic_int.geo_to_h3shape(mapping(geom)),
            resolution,
            _CONTAIN[self.mode],
        )

    def _cell_at(self, lon: float, lat: float, resolution: int) -> int:
        return basic_int.latlng_to_cell(lat, lon, resolution)

    def _neighbours(self, cell: str | int) -> list[int]:
        """
        Hexagons have no vertex-only neighbours, so the edge-adjacent disk
        is already what _cover_segment needs.
        """
        return basic_int.grid_disk(_as_int(cell), 1)

    def _linetrace(self, geom, resolution: int) -> list:
        """
        h3-py has no line cover: grid_path_cells walks a route between two
        cells, not the cells a line occupies.
        """
        return self._cover_line(geom, resolution)

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
            lambda geom, res: [basic_int.latlng_to_cell(geom.y, geom.x, res)],
            df.geometry.name,
        )

    def secondary_index(self, df: pd.DataFrame, parent_res: int) -> pd.DataFrame:
        df[f"h3_{parent_res:02}"] = df.index.map(
            lambda cell: basic_int.cell_to_parent(int(cell), parent_res)
        ).astype("uint64")
        return df

    def compaction(self, df, res, col_order, dggs_col, id_field, parent_res):
        def _compact(cells):
            return basic_int.compact_cells([int(c) for c in cells])

        def _child(cell, res):
            return basic_int.cell_to_center_child(int(cell), res)

        return self.compaction_common(
            df,
            res,
            id_field,
            col_order,
            dggs_col,
            _compact,
            _child,
            parent_res,
            self.get_resolution,
            self.children_at_res,
        )

    @staticmethod
    def get_resolution(cell: str | int) -> int:
        """
        Returns the resolution of a cell (native int, or string token).

        Not a part of the interface provided by VectorIndexer.
        """
        return basic_int.get_resolution(_as_int(cell))

    @staticmethod
    def children_at_res(cell: str | int, target_res: int) -> list[int]:
        """
        Return all descendants of cell (native int, or string token) at
        resolution target_res.

        Not a part of the interface provided by VectorIndexer.
        """
        return basic_int.cell_to_children(_as_int(cell), target_res)

    @staticmethod
    def cell_to_point(cell: str | int) -> Point:
        return Point(basic_int.cell_to_latlng(_as_int(cell))[::-1])

    def cells_to_polygons(self, cells: Iterable[str | int]) -> Iterable[Polygon]:
        """
        Batched construction, which dominates the per-cell cost of the
        line cover: h3-py has no batch boundary call, but building the
        polygons from one array beats building them one at a time.
        Boundaries are grouped by vertex count, since a pentagon or a
        distorted cell has a different number from a plain hexagon.
        """
        boundaries = [basic_int.cell_to_boundary(_as_int(c)) for c in cells]
        polygons = np.empty(len(boundaries), dtype=object)
        by_size: dict[int, list[int]] = {}
        for i, boundary in enumerate(boundaries):
            by_size.setdefault(len(boundary), []).append(i)
        for size, positions in by_size.items():
            rings = np.empty((len(positions), size, 2))
            for k, i in enumerate(positions):
                rings[k] = [coord[::-1] for coord in boundaries[i]]
            polygons[positions] = shapely.polygons(rings)
        return polygons

    @staticmethod
    def cell_to_polygon(cell: str | int) -> Polygon:
        return Polygon(
            tuple(coord[::-1] for coord in basic_int.cell_to_boundary(_as_int(cell)))
        )
