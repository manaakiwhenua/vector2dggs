import h3
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon, mapping

from vector2dggs.indexers.vectorindexer import VectorIndexer


class H3VectorIndexer(VectorIndexer):
    """
    Provides integration for Uber's H3 DGGS.
    """

    @staticmethod
    def _polyfill_polygon(geom, resolution: int) -> list:
        return h3.geo_to_cells(mapping(geom), resolution)

    @staticmethod
    def _linetrace(geom, resolution: int) -> list:
        coords = list(geom.coords)
        cells = set()
        for i in range(len(coords) - 1):
            start = h3.latlng_to_cell(coords[i][1], coords[i][0], resolution)
            end = h3.latlng_to_cell(coords[i + 1][1], coords[i + 1][0], resolution)
            cells.update(h3.grid_path_cells(start, end))
        return list(cells)

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
                    lambda geom, res: [h3.latlng_to_cell(geom.y, geom.x, res)],
                    geom_col,
                )
            )

        return pd.concat(parts) if parts else pd.DataFrame()

    def secondary_index(self, df: pd.DataFrame, parent_res: int) -> pd.DataFrame:
        df[f"h3_{parent_res:02}"] = df.index.map(
            lambda cell: h3.cell_to_parent(cell, parent_res)
        )
        return df

    def compaction(self, df, res, col_order, dggs_col, id_field):
        return self.compaction_common(
            df,
            res,
            id_field,
            col_order,
            dggs_col,
            h3.compact_cells,
            h3.cell_to_center_child,
        )

    @staticmethod
    def cell_to_point(cell: str) -> Point:
        return Point(h3.cell_to_latlng(cell)[::-1])

    @staticmethod
    def cell_to_polygon(cell: str) -> Polygon:
        return Polygon(tuple(coord[::-1] for coord in h3.cell_to_boundary(cell)))
