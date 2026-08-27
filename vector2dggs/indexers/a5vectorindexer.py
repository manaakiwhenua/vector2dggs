import a5
import geopandas as gpd
import pandas as pd
import pyarrow as pa
from shapely.geometry import Point, Polygon

from vector2dggs.indexers.vectorindexer import VectorIndexer


def _as_u64(cell) -> int:
    """
    Accepts either form: the working native uint64 (from within the
    pipeline), or the hex string (e.g. reading a --cell-id string output
    file back and using these methods as a standalone convenience, the way
    tests do against the pipeline's own default output).
    """
    return a5.hex_to_u64(cell) if isinstance(cell, str) else int(cell)


class A5VectorIndexer(VectorIndexer):
    """
    Provides integration for the A5 pentagonal DGGS.

    pya5 cells are natively uint64; the hexadecimal form is produced only
    at the output boundary via cells_to_string.
    """

    GEODESIC_POLYFILL = True
    CELL_ARROW_TYPE: pa.DataType = pa.uint64()

    @staticmethod
    def cells_to_string(cells) -> list:
        return [a5.u64_to_hex(int(c)) for c in cells]

    @staticmethod
    def _polyfill_polygon(geom, resolution: int) -> list:
        interiors = [i.coords for i in geom.interiors]
        return list(
            set(
                a5.uncompact(
                    a5.polygon_to_cells([geom.exterior.coords, *interiors], resolution),
                    resolution,
                )
            )
        )

    @staticmethod
    def _linetrace(geom, resolution: int) -> list:
        # set: no documented uniqueness guarantee from line_string_to_cells
        return list(set(a5.line_string_to_cells(list(geom.coords), resolution)))

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
            lambda geom, res: [a5.lonlat_to_cell((geom.x, geom.y), res)],
            df.geometry.name,
        )

    def secondary_index(self, df: pd.DataFrame, parent_res: int) -> pd.DataFrame:
        df[f"a5_{parent_res:02}"] = df.index.map(
            lambda cell: a5.cell_to_parent(int(cell), parent_res)
        ).astype("uint64")
        return df

    def compaction(self, df, res, col_order, dggs_col, id_field, parent_res):
        def _compact(cells):
            return list(a5.compact([int(c) for c in cells]))

        def _child(cell, res):
            return a5.cell_to_children(int(cell), res)[0]

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
    def get_resolution(cell) -> int:
        """
        Returns the resolution of a cell (native uint64, or hex string).

        Not a part of the interface provided by VectorIndexer.
        """
        return a5.get_resolution(_as_u64(cell))

    @staticmethod
    def children_at_res(cell, target_res: int) -> list:
        """
        Return all descendants of a cell (native uint64, or hex string) at
        target_res.

        Not a part of the interface provided by VectorIndexer.
        """
        cell = _as_u64(cell)
        if a5.get_resolution(cell) >= target_res:
            return [cell]
        return list(a5.cell_to_children(cell, target_res))

    @staticmethod
    def cell_to_point(cell) -> Point:
        lon, lat = a5.cell_to_lonlat(_as_u64(cell))
        return Point(lon, lat)

    @staticmethod
    def cell_to_polygon(cell) -> Polygon:
        return Polygon(a5.cell_to_boundary(_as_u64(cell)))
