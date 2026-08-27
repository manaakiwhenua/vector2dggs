from collections.abc import Iterable

import geopandas as gpd
import pandas as pd
import pyarrow as pa
import s2geometry as S2
from shapely import force_2d
from shapely.geometry import LineString, Point, Polygon

from vector2dggs.indexers.vectorindexer import VectorIndexer


def _cell_from(cell: str | int) -> S2.S2CellId:
    """
    Accepts either form: the working native uint64 (from within the
    pipeline), or the token string (e.g. reading a --cell-id string output
    file back and using these methods as a standalone convenience, the way
    tests do against the pipeline's own default output).
    """
    return (
        S2.S2CellId.FromToken(cell) if isinstance(cell, str) else S2.S2CellId(int(cell))
    )


def _cell_id(cell: S2.S2CellId) -> int:
    return cell.id()


class S2VectorIndexer(VectorIndexer[str | int]):
    """
    Provides integration for Google's S2 DGGS.

    S2CellId is natively uint64; the token (hex string) form is produced
    only at the output boundary via cells_to_string.
    """

    GEODESIC_POLYFILL = True
    CELL_ARROW_TYPE: pa.DataType = pa.uint64()

    @staticmethod
    def cells_to_string(cells: Iterable[str | int]) -> list[str]:
        return [S2.S2CellId(int(c)).ToToken() for c in cells]

    def _polyfill_polygons(self, df: gpd.GeoDataFrame, level: int) -> pd.DataFrame:
        return self._geo_to_cells(df, level, self.cells_from_polygon, df.geometry.name)

    def _polyfill_linestrings(self, df: gpd.GeoDataFrame, level: int) -> pd.DataFrame:
        return self._geo_to_cells(
            df, level, self.cells_from_linestring, df.geometry.name
        )

    def _polyfill_points(self, df: gpd.GeoDataFrame, level: int) -> pd.DataFrame:
        return self._geo_to_cells(
            df,
            level,
            lambda geom, lvl: [self.cell_from_point(geom, lvl)],
            df.geometry.name,
        )

    def secondary_index(self, df: pd.DataFrame, parent_level: int) -> pd.DataFrame:
        """
        Implementation of abstract function.
        """
        df[f"s2_{parent_level:02}"] = (
            df.index.to_series()
            .map(lambda cell: _cell_from(cell).parent(parent_level).id())
            .astype("uint64")
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
            self.compact_cells,
            self.cell_to_child_cell,
            parent_res,
            self.get_resolution,
            self.children_at_res,
        )

    def cells_from_polygon(
        self, geom: Polygon, level: int, centroid_inside: bool = True
    ) -> set[int]:
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

        return {_cell_id(cell) for cell in covering}

    def cell_center_is_inside_polygon(
        self, cell: S2.S2CellId, polygon: S2.S2Polygon
    ) -> bool:
        """
        Determines if the center of the S2 cell is inside the polygon

        Not a part of the interface provided by VectorIndexer.
        """
        cell_center = S2.S2Cell(cell).GetCenter()
        return polygon.Contains(cell_center)

    def cells_from_linestring(self, linestring: LineString, level: int) -> list[int]:
        """
        Not a part of the interface provided by VectorIndexer.
        """

        latlngs = [S2.S2LatLng.FromDegrees(lat, lon) for lon, lat in linestring.coords]
        polyline = S2.S2Polyline()
        polyline.InitFromS2LatLngs(latlngs)

        coverer = S2.S2RegionCoverer()
        coverer.set_min_level(level)
        coverer.set_max_level(level)

        return [_cell_id(cell) for cell in coverer.GetCovering(polyline)]

    def cell_from_point(self, geom: Point, level: int) -> int:
        """
        Convert a point geometry to an S2 cell ID at the specified level.

        Not a part of the interface provided by VectorIndexer.
        """
        latlng = S2.S2LatLng.FromDegrees(geom.y, geom.x)
        return _cell_id(S2.S2CellId(latlng).parent(level))

    def compact_cells(self, cells: Iterable[str | int]) -> set[int]:
        """
        Compact a set of S2 DGGS cells.
        Cells must be at the same resolution.

        Not a part of the interface provided by VectorIndexer.
        """
        cell_ids: list[S2.S2CellId] = [_cell_from(cell) for cell in cells]
        cell_union: S2.S2CellUnion = S2.S2CellUnion(
            cell_ids
        )  # Vector of sorted, non-overlapping S2CellId
        cell_union.NormalizeS2CellUnion()  # Mutates; 'normalize' == 'compact'
        return {_cell_id(c) for c in cell_union.cell_ids()}

    @staticmethod
    def get_resolution(cell: str | int) -> int:
        """
        Returns the level of a cell (native uint64, or token string).

        Not a part of the interface provided by VectorIndexer.
        """
        return _cell_from(cell).level()

    @staticmethod
    def children_at_res(cell: str | int, target_level: int) -> list[int]:
        """
        Return all descendants of a cell (native uint64, or token string) at
        target_level.

        Not a part of the interface provided by VectorIndexer.
        """
        cell_id: S2.S2CellId = _cell_from(cell)
        if target_level <= cell_id.level():
            return [_cell_id(cell_id)]
        end = cell_id.child_end(target_level)
        ids = []
        cur = cell_id.child_begin(target_level)
        while cur != end:
            ids.append(_cell_id(cur))
            cur = cur.next()
        return ids

    def cell_to_child_cell(self, cell: str | int, level: int) -> int:
        """
        Returns first child (native uint64) of a cell (native uint64, or
        token string) at a specific level.

        Not a part of the interface provided by VectorIndexer.
        """
        cell_id: S2.S2CellId = _cell_from(cell)
        if level <= cell_id.level():
            raise ValueError(
                "Level must be greater than the current level of the cell."
            )
        # Get the child cell iterator
        return _cell_id(cell_id.child_begin(level))

    @staticmethod
    def cell_to_point(cell: str | int) -> Point:
        cell_id = _cell_from(cell)
        latlng = cell_id.ToLatLng()
        return Point(latlng.lng().degrees(), latlng.lat().degrees())

    @staticmethod
    def cell_to_polygon(cell: str | int) -> Polygon:
        s2_cell = S2.S2Cell(_cell_from(cell))
        return Polygon(
            tuple(
                (
                    vertex.lng().degrees(),
                    vertex.lat().degrees(),
                )
                for vertex in (s2_cell.GetS2LatLngVertex(i) for i in range(4))
            )
        )
