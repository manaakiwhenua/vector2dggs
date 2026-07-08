import a5
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon

from vector2dggs.indexers.vectorindexer import VectorIndexer


class A5VectorIndexer(VectorIndexer):
    """
    Provides integration for the A5 pentagonal DGGS.
    """

    @staticmethod
    def _polyfill_polygon(geom, resolution: int) -> list:
        interiors = [i.coords for i in geom.interiors]
        cells = set(
            a5.uncompact(
                a5.polygon_to_cells(
                    [geom.exterior.coords, *interiors], resolution
                ),
                resolution,
            )
        )
        return [a5.u64_to_hex(c) for c in cells]

    @staticmethod
    def _linetrace(geom, resolution: int) -> list:
        return [
            a5.u64_to_hex(c)
            for c in a5.line_string_to_cells(list(geom.coords), resolution)
        ]

    def polyfill(self, df: gpd.GeoDataFrame, resolution: int) -> pd.DataFrame:
        geom_col = df.geometry.name
        parts = []

        df_polygon = df[df.geom_type == "Polygon"]
        if not df_polygon.empty:
            parts.append(
                self._geo_to_cells(
                    df_polygon, resolution, self._polyfill_polygon, geom_col
                )
            )

        df_linestring = df[df.geom_type == "LineString"]
        if not df_linestring.empty:
            ls = self._geo_to_cells(
                df_linestring, resolution, self._linetrace, geom_col
            )
            parts.append(ls[~ls.index.duplicated(keep="first")])

        df_point = df[df.geom_type == "Point"]
        if not df_point.empty:
            parts.append(
                self._geo_to_cells(
                    df_point,
                    resolution,
                    lambda geom, res: [
                        a5.u64_to_hex(a5.lonlat_to_cell((geom.x, geom.y), res))
                    ],
                    geom_col,
                )
            )

        return pd.concat(parts) if parts else pd.DataFrame()

    def secondary_index(self, df: pd.DataFrame, parent_res: int) -> pd.DataFrame:
        df[f"a5_{parent_res:02}"] = df.index.map(
            lambda cell: a5.u64_to_hex(
                a5.cell_to_parent(a5.hex_to_u64(cell), parent_res)
            )
        )
        return df

    def compaction(self, df, res, col_order, dggs_col, id_field):
        def _compact_hex(cells):
            return [
                a5.u64_to_hex(c) for c in a5.compact([a5.hex_to_u64(c) for c in cells])
            ]

        def _child_hex(cell, res):
            return a5.u64_to_hex(a5.cell_to_children(a5.hex_to_u64(cell), res)[0])

        return self.compaction_common(
            df, res, id_field, col_order, dggs_col, _compact_hex, _child_hex
        )

    @staticmethod
    def cell_to_point(cell: str) -> Point:
        lon, lat = a5.cell_to_lonlat(a5.hex_to_u64(cell))
        return Point(lon, lat)

    @staticmethod
    def cell_to_polygon(cell: str) -> Polygon:
        return Polygon(a5.cell_to_boundary(a5.hex_to_u64(cell)))
