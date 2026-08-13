import a5
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, Polygon

from vector2dggs.indexers.vectorindexer import VectorIndexer


class A5VectorIndexer(VectorIndexer):
    """
    Provides integration for the A5 pentagonal DGGS.
    """

    GEODESIC_POLYFILL = True

    @staticmethod
    def _polyfill_polygon(geom, resolution: int) -> list:
        interiors = [i.coords for i in geom.interiors]
        cells = set(
            a5.uncompact(
                a5.polygon_to_cells([geom.exterior.coords, *interiors], resolution),
                resolution,
            )
        )
        return [a5.u64_to_hex(c) for c in cells]

    @staticmethod
    def _linetrace(geom, resolution: int) -> list:
        # set: no documented uniqueness guarantee from line_string_to_cells
        return [
            a5.u64_to_hex(c)
            for c in set(a5.line_string_to_cells(list(geom.coords), resolution))
        ]

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
            lambda geom, res: [a5.u64_to_hex(a5.lonlat_to_cell((geom.x, geom.y), res))],
            df.geometry.name,
        )

    def secondary_index(self, df: pd.DataFrame, parent_res: int) -> pd.DataFrame:
        df[f"a5_{parent_res:02}"] = df.index.map(
            lambda cell: a5.u64_to_hex(
                a5.cell_to_parent(a5.hex_to_u64(cell), parent_res)
            )
        )
        return df

    def compaction(self, df, res, col_order, dggs_col, id_field, parent_res):
        def _compact_hex(cells):
            return [
                a5.u64_to_hex(c) for c in a5.compact([a5.hex_to_u64(c) for c in cells])
            ]

        def _child_hex(cell, res):
            return a5.u64_to_hex(a5.cell_to_children(a5.hex_to_u64(cell), res)[0])

        return self.compaction_common(
            df,
            res,
            id_field,
            col_order,
            dggs_col,
            _compact_hex,
            _child_hex,
            parent_res,
            self.get_resolution,
            self.children_at_res,
        )

    @staticmethod
    def get_resolution(cell: str) -> int:
        """
        Returns the resolution of a cell (represented as a hex string).

        Not a part of the interface provided by VectorIndexer.
        """
        return a5.get_resolution(a5.hex_to_u64(cell))

    @staticmethod
    def children_at_res(cell: str, target_res: int) -> list[str]:
        """
        Return all descendants of a cell (represented as a hex string) at
        target_res.

        Not a part of the interface provided by VectorIndexer.
        """
        u64 = a5.hex_to_u64(cell)
        if a5.get_resolution(u64) >= target_res:
            return [cell]
        return [a5.u64_to_hex(c) for c in a5.cell_to_children(u64, target_res)]

    @staticmethod
    def cell_to_point(cell: str) -> Point:
        lon, lat = a5.cell_to_lonlat(a5.hex_to_u64(cell))
        return Point(lon, lat)

    @staticmethod
    def cell_to_polygon(cell: str) -> Polygon:
        return Polygon(a5.cell_to_boundary(a5.hex_to_u64(cell)))
