from collections.abc import Iterable

import geopandas as gpd
import pandas as pd
import s2geometry as S2
from shapely import force_2d
from shapely.geometry import LineString, Point, Polygon

from vector2dggs.indexers.vectorindexer import VectorIndexer


class S2VectorIndexer(VectorIndexer):
    """
    Provides integration for Google's S2 DGGS.
    """

    GEODESIC_POLYFILL = True

    def _polyfill_polygons(self, df: gpd.GeoDataFrame, level: int) -> pd.DataFrame:
        return self._geo_to_cells(df, level, self.tokens_from_polygon, df.geometry.name)

    def _polyfill_linestrings(self, df: gpd.GeoDataFrame, level: int) -> pd.DataFrame:
        return self._geo_to_cells(
            df, level, self.tokens_from_linestring, df.geometry.name
        )

    def _polyfill_points(self, df: gpd.GeoDataFrame, level: int) -> pd.DataFrame:
        return self._geo_to_cells(
            df,
            level,
            lambda geom, lvl: [self.token_from_point(geom, lvl)],
            df.geometry.name,
        )

    def secondary_index(self, df: pd.DataFrame, parent_level: int) -> pd.DataFrame:
        """
        Implementation of abstract function.
        """
        df[f"s2_{parent_level:02}"] = df.index.to_series().map(
            lambda token: S2.S2CellId.FromToken(token).parent(parent_level).ToToken()
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
            parent_res,
            self.get_resolution,
            self.children_at_res,
        )

    def tokens_from_polygon(
        self, geom: Polygon, level: int, centroid_inside: bool = True
    ) -> set[str]:
        """
        Not a part of the interface provided by VectorIndexer.
        """
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
        # (min_level == max_level, so max_cells is irrelevant and left unset)
        coverer = S2.S2RegionCoverer()
        coverer.set_min_level(level)
        coverer.set_max_level(level)

        raw_covering: Iterable[S2.S2CellId] = coverer.GetCovering(s2polygon)
        covering: set[S2.S2CellId]

        if centroid_inside:
            # Coverings are "intersects" modality, polyfill is "centre inside" modality
            # ergo, filter out covering cells that are not inside the polygon
            covering = {
                cell
                for cell in raw_covering
                if self.cell_center_is_inside_polygon(cell, s2polygon)
            }
        else:
            covering = set(raw_covering)

        return {cell.ToToken() for cell in covering}

    def cell_center_is_inside_polygon(
        self, cell: S2.S2CellId, polygon: S2.S2Polygon
    ) -> bool:
        """
        Determines if the center of the S2 cell is inside the polygon

        Not a part of the interface provided by VectorIndexer.
        """
        cell_center = S2.S2Cell(cell).GetCenter()
        return polygon.Contains(cell_center)

    def tokens_from_linestring(self, linestring: LineString, level: int) -> list[str]:
        """
        Not a part of the interface provided by VectorIndexer.
        """

        latlngs = [S2.S2LatLng.FromDegrees(lat, lon) for lon, lat in linestring.coords]
        polyline = S2.S2Polyline()
        polyline.InitFromS2LatLngs(latlngs)

        coverer = S2.S2RegionCoverer()
        coverer.set_min_level(level)
        coverer.set_max_level(level)

        return [cell.ToToken() for cell in coverer.GetCovering(polyline)]

    def token_from_point(self, geom: Point, level: int) -> str:
        """
        Convert a point geometry to an S2 cell token at the specified level.

        Not a part of the interface provided by VectorIndexer.
        """
        latlng = S2.S2LatLng.FromDegrees(geom.y, geom.x)
        return S2.S2CellId(latlng).parent(level).ToToken()

    def compact_tokens(self, tokens: Iterable[str]) -> set[str]:
        """
        Compact a set of S2 DGGS cells.
        Cells must be at the same resolution.

        Not a part of the interface provided by VectorIndexer.
        """
        cell_ids: list[S2.S2CellId] = [S2.S2CellId.FromToken(token) for token in tokens]
        cell_union: S2.S2CellUnion = S2.S2CellUnion(
            cell_ids
        )  # Vector of sorted, non-overlapping S2CellId
        cell_union.NormalizeS2CellUnion()  # Mutates; 'normalize' == 'compact'
        return {c.ToToken() for c in cell_union.cell_ids()}

    @staticmethod
    def get_resolution(token: str) -> int:
        """
        Returns the level of a cell (represented as a string token).

        Not a part of the interface provided by VectorIndexer.
        """
        return S2.S2CellId.FromToken(token).level()

    @staticmethod
    def children_at_res(token: str, target_level: int) -> list[str]:
        """
        Return all descendants of a cell (represented as a string token) at
        target_level.

        Not a part of the interface provided by VectorIndexer.
        """
        cell: S2.S2CellId = S2.S2CellId.FromToken(token)
        if target_level <= cell.level():
            return [token]
        end = cell.child_end(target_level)
        tokens = []
        cur = cell.child_begin(target_level)
        while cur != end:
            tokens.append(cur.ToToken())
            cur = cur.next()
        return tokens

    def token_to_child_token(self, token: str, level: int) -> str:
        """
        Returns first child (as string token) of a cell (also represented as a
        string token) at a specific level.

        Not a part of the interface provided by VectorIndexer.
        """
        cell: S2.S2CellId = S2.S2CellId.FromToken(token)
        if level <= cell.level():
            raise ValueError(
                "Level must be greater than the current level of the cell."
            )
        # Get the child cell iterator
        return cell.child_begin(level).ToToken()

    @staticmethod
    def cell_to_point(cell: str) -> Point:
        cell_id = S2.S2CellId.FromToken(cell)
        latlng = cell_id.ToLatLng()
        return Point(latlng.lng().degrees(), latlng.lat().degrees())

    @staticmethod
    def cell_to_polygon(cell: str) -> Polygon:
        s2_cell = S2.S2Cell(S2.S2CellId.FromToken(cell))
        return Polygon(
            tuple(
                (
                    vertex.lng().degrees(),
                    vertex.lat().degrees(),
                )
                for vertex in (s2_cell.GetS2LatLngVertex(i) for i in range(4))
            )
        )
