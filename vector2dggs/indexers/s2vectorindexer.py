"""

@author: ndemaio
"""

from typing import Union
from math import ceil

import pandas as pd
import geopandas as gpd

from s2geometry import pywraps2 as S2
from shapely import force_2d
from shapely.geometry import box, Polygon, LineString, Point
from shapely.ops import transform
from pyproj import CRS, Transformer

import vector2dggs.constants as const

from vector2dggs.indexers.vectorindexer import VectorIndexer


class S2VectorIndexer(VectorIndexer):
    """
    Provides integration for Google's S2 DGGS.
    """

    def polyfill(self, df: gpd.GeoDataFrame, level: int) -> pd.DataFrame:
        """
        Implementation of abstract function.
        """

        df_polygon = df[df.geom_type == "Polygon"].copy()
        if len(df_polygon.index) > 0:
            df_polygon = (
                self.polyfill_polygons(df_polygon, level)
                .explode("s2index")
                .set_index("s2index")
            )

        df_linestring = df[df.geom_type == "LineString"].copy()
        if len(df_linestring.index) > 0:
            df_linestring["s2index"] = df_linestring.geometry.apply(
                lambda geom: self.cell_ids_from_linestring(geom, level)
            )
            df_linestring = df_linestring.explode("s2index").set_index("s2index")

        df_point = df[df.geom_type == "Point"].copy()
        if len(df_point.index) > 0:
            df_point["s2index"] = df_point.geometry.apply(
                lambda geom: self.cell_id_from_point(geom, level)
            )
            df_point = df_point.set_index("s2index")

        return pd.concat(
            map(
                lambda _df: pd.DataFrame(_df.drop(columns=[_df.geometry.name])),
                [df_polygon, df_linestring, df_point],
            )
        )

    def secondary_index(self, df: pd.DataFrame, parent_level: int) -> pd.DataFrame:
        """
        Implementation of abstract function.
        """

        # NB also converts the index to S2 cell tokens
        index_series = df.index.to_series().astype(object)
        df[f"s2_{parent_level:02}"] = index_series.map(
            lambda cell_id: cell_id.parent(parent_level).ToToken()
        )
        df.index = index_series.map(lambda cell_id: cell_id.ToToken())
        return df

    def compaction(
        self,
        df: pd.DataFrame,
        res: int,
        col_order: list,
        dggs_col: str,
        id_field: str,
    ) -> pd.DataFrame:
        """
        Compacts an S2 dataframe up to a given low resolution (parent_res),
        from an existing maximum resolution (res).

        Implementation of abstract function.
        """
        return self.compaction_common(
            df,
            res,
            id_field,
            col_order,
            dggs_col,
            self.compact_tokens,
            self.token_to_child_token,
        )

    def polyfill_polygons(self, df: gpd.GeoDataFrame, level: int) -> gpd.GeoDataFrame:
        """
        Not a part of the interface provided by VectorIndexer.
        """

        def generate_covering(
            geom: Polygon, level: int, centroid_inside: bool = True
        ) -> set[S2.S2CellId]:
            geom = force_2d(geom)
            # Prepare loops: first the exterior loop, then the interior loops
            loops = []
            # Exterior ring
            latlngs = [
                S2.S2LatLng.FromDegrees(lat, lon) for lon, lat in geom.exterior.coords
            ]
            s2loop = S2.S2Loop([latlng.ToPoint() for latlng in latlngs])
            s2loop.Normalize()
            loops.append(s2loop)

            # Interior rings (polygon holes)
            for interior in geom.interiors:
                interior_latlngs = [
                    S2.S2LatLng.FromDegrees(lat, lon) for lon, lat in interior.coords
                ]
                s2interior_loop = S2.S2Loop(
                    [latlng.ToPoint() for latlng in interior_latlngs]
                )
                s2interior_loop.Normalize()
                loops.append(s2interior_loop)

            # Build an S2Polygon from the loops
            s2polygon = S2.S2Polygon()
            s2polygon.InitNested(loops)

            # Use S2RegionCoverer to get the cell IDs at the specified level
            coverer = S2.S2RegionCoverer()

            max_cells = self.max_cells_for_geom(geom, level)
            coverer.set_max_cells(max_cells)
            coverer.set_min_level(level)
            coverer.set_max_level(level)

            covering: list[S2.S2CellId] = coverer.GetCovering(s2polygon)

            if centroid_inside:
                # Coverings are "intersects" modality, polyfill is "centre inside" modality
                # ergo, filter out covering cells that are not inside the polygon
                covering = {
                    cell
                    for cell in covering
                    if self.cell_center_is_inside_polygon(cell, s2polygon)
                }
            else:
                set(covering)

            return covering

        df["s2index"] = df["geometry"].apply(
            lambda geom: generate_covering(geom, level)
        )
        df = df[
            df["s2index"].map(lambda x: len(x) > 0)
        ]  # Remove rows with no covering at this level

        return df

    def max_cells_for_geom(
        self, geom: Union[Polygon, LineString], level: int, margin: float = 1.02
    ) -> int:
        """
        Calculate the maximum number of S2 cells that are appropriate for the given geometry and level.
        This is based on the area of the geometry's bounding box,
        and the maximum area of S2 cells at the given level.

        Not a part of the interface provided by VectorIndexer.
        """
        area = self.bbox_area_in_m2(geom)
        max_cells = ceil(max(1, area / const.S2_CELLS_MAX_AREA_M2_BY_LEVEL[level]))
        return ceil(max_cells * margin)

    def bbox_area_in_m2(
        self,
        geom: Polygon,
        src_crs: Union[str, CRS] = "EPSG:4326",
        dst_crs: Union[str, CRS] = "EPSG:6933",
    ) -> float:
        """
        Calculate the area of the bounding box of a geometry in square meters.

        Not a part of the interface provided by VectorIndexer.
        """
        minx, miny, maxx, maxy = geom.bounds
        bbox = box(minx, miny, maxx, maxy)
        transformer = Transformer.from_crs(src_crs, dst_crs, always_xy=True)
        projected_bbox = transform(transformer.transform, bbox)
        return projected_bbox.area

    def cell_center_is_inside_polygon(
        self, cell: S2.S2CellId, polygon: S2.S2Polygon
    ) -> bool:
        """
        Determines if the center of the S2 cell is inside the polygon

        Not a part of the interface provided by VectorIndexer.
        """
        cell_center = S2.S2Cell(cell).GetCenter()
        return polygon.Contains(cell_center)

    def cell_ids_from_linestring(
        self, linestring: LineString, level: int
    ) -> list[S2.S2CellId]:
        """
        Not a part of the interface provided by VectorIndexer.
        """

        latlngs = [S2.S2LatLng.FromDegrees(lat, lon) for lon, lat in linestring.coords]
        polyline = S2.S2Polyline(latlngs)

        coverer = S2.S2RegionCoverer()
        max_cells = self.max_cells_for_geom(linestring, level)
        coverer.set_max_cells(max_cells)
        coverer.set_min_level(level)
        coverer.set_max_level(level)

        return coverer.GetCovering(polyline)

    def cell_id_from_point(self, geom: Point, level: int) -> S2.S2CellId:
        """
        Convert a point geometry to an S2 cell at the specified level.

        Not a part of the interface provided by VectorIndexer.
        """
        latlng = S2.S2LatLng.FromDegrees(geom.y, geom.x)
        return S2.S2CellId(latlng).parent(level)

    def compact_tokens(self, tokens: set[str]) -> set[str]:
        """
        Compact a set of S2 DGGS cells.
        Cells must be at the same resolution.

        Not a part of the interface provided by VectorIndexer.
        """
        cell_ids: list[S2.S2CellId] = [
            S2.S2CellId.FromToken(token, len(token)) for token in tokens
        ]
        cell_union: S2.S2CellUnion = S2.S2CellUnion(
            cell_ids
        )  # Vector of sorted, non-overlapping S2CellId
        cell_union.NormalizeS2CellUnion()  # Mutates; 'normalize' == 'compact'
        return {c.ToToken() for c in cell_union.cell_ids()}

    def token_to_child_token(self, token: str, level: int) -> str:
        """
        Returns first child (as string token) of a cell (also represented as a
        string token) at a specific level.

        Not a part of the interface provided by VectorIndexer.
        """
        cell: S2.S2CellId = S2.S2CellId.FromToken(token, len(token))
        if level <= cell.level():
            raise ValueError(
                "Level must be greater than the current level of the cell."
            )
        # Get the child cell iterator
        return cell.child_begin(level).ToToken()
