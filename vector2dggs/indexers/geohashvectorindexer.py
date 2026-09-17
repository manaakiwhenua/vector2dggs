from collections.abc import Iterable
from itertools import product

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
from geohash import decode, decode_exactly, encode, neighbors  # python-geohash
from geohash_polygon import polygon_to_geohashes  # rusty-polygon-geohasher
from shapely.geometry import Point, Polygon, box

import vector2dggs.constants as const
from vector2dggs.indexers.vectorindexer import VectorIndexer


class GeohashVectorIndexer(VectorIndexer[str]):
    """
    Provides integration for the Geohash geocode system.
    """

    GEODESIC_POLYFILL = False

    GEOHASH_BASE32_SET = set("0123456789bcdefghjkmnpqrstuvwxyz")

    def _polyfill_polygons(self, df: gpd.GeoDataFrame, level: int) -> pd.DataFrame:
        geom_col = df.geometry.name
        gh_col = "geohash"
        result = (
            df.assign(
                **{
                    gh_col: df.geometry.apply(
                        lambda geom: self._polygon_to_geohashes(geom, level)
                    )
                }
            )
            .drop(columns=[geom_col])
            .explode(gh_col, ignore_index=True)
            .dropna(subset=[gh_col])
            .set_index(gh_col)
        )
        return pd.DataFrame(result)

    def _polyfill_linestrings(self, df: gpd.GeoDataFrame, level: int) -> pd.DataFrame:
        geom_col = df.geometry.name
        gh_col = "geohash"
        result = (
            df.assign(
                **{
                    gh_col: df.geometry.apply(
                        lambda geom: self._cover_line(geom, level)
                    )
                }
            )
            .drop(columns=[geom_col])
            .explode(gh_col, ignore_index=True)
            .dropna(subset=[gh_col])
            .set_index(gh_col)
        )
        return pd.DataFrame(result)

    def _polyfill_points(self, df: gpd.GeoDataFrame, level: int) -> pd.DataFrame:
        geom_col = df.geometry.name
        gh_col = "geohash"
        result = (
            df.assign(
                **{
                    gh_col: df.geometry.apply(
                        lambda geom: encode(geom.y, geom.x, precision=level)
                    )
                }
            )
            .drop(columns=[geom_col])
            .set_index(gh_col)
        )
        return pd.DataFrame(result)

    def _cell_at(self, lon: float, lat: float, resolution: int) -> str:
        return encode(lat, lon, precision=resolution)

    def _neighbours(self, cell: str) -> list[str]:
        """
        All eight, diagonals included, so corner-only neighbours count.
        """
        return neighbors(cell)

    def secondary_index(self, df: pd.DataFrame, parent_level: int) -> pd.DataFrame:
        """
        Implementation of abstract function.
        """
        dggs_col = f"geohash_{parent_level:02}"
        df[dggs_col] = df.index.to_series().astype(str).str[:parent_level]
        df[dggs_col] = df[dggs_col].astype(str)
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
        Compacts a geohash dataframe up to a given low resolution (parent_res),
        from an existing maximum resolution (res).

        Implementation of abstract function.
        """
        return self.compaction_common(
            df,
            res,
            id_field,
            col_order,
            dggs_col,
            self.compact,
            self.get_child_geohash,
            parent_res,
            self.get_resolution,
            self.children_at_res,
        )

    def compact(self, cells: Iterable[str]) -> set[str]:
        """
        Compact a set of geohash cells.
        Cells must be at the same resolution.

        Not a part of the interface provided by VectorIndexer.
        """
        current_set = set(cells)
        # Discard any null values
        current_set = {c for c in current_set if pd.notna(c)}
        while True:
            parent_map: dict[str, set[str]] = {}
            for gh in current_set:
                parent = gh[:-1]
                if parent not in parent_map:
                    parent_map[parent] = set()
                parent_map[parent].add(gh)

            next_set = set()
            for parent, siblings in parent_map.items():
                if len(siblings) == 32:
                    next_set.add(parent)
                else:
                    next_set.update(siblings)

            if next_set == current_set:
                break
            current_set = next_set

        return current_set

    @staticmethod
    def get_resolution(cell: str) -> int:
        """
        Returns the resolution (length) of a geohash.

        Not a part of the interface provided by VectorIndexer.
        """
        return len(cell)

    @staticmethod
    def children_at_res(geohash: str, target_res: int) -> list[str]:
        """
        Return all descendants of geohash at length target_res.

        Not a part of the interface provided by VectorIndexer.
        """
        if target_res <= len(geohash):
            return [geohash]
        chars = sorted(GeohashVectorIndexer.GEOHASH_BASE32_SET)
        return [
            geohash + "".join(suffix)
            for suffix in product(chars, repeat=target_res - len(geohash))
        ]

    def get_child_geohash(self, geohash: str, desired_length: int, child: str = "0"):
        """
        Get a child geohash of the specified length by extending the input geohash.

        Not a part of the interface provided by VectorIndexer.
        """
        if child not in self.GEOHASH_BASE32_SET:
            raise ValueError(
                f"Invalid child character '{child}'. Must be one of {''.join(self.GEOHASH_BASE32_SET)}."
            )

        if len(geohash) >= desired_length:
            return geohash
        return geohash.ljust(desired_length, child)

    def _polygon_to_geohashes(self, polygon: Polygon, level: int) -> set[str]:
        """
        The geohash set for one polygon, under this indexer's mode.

        geohash_polygon tests boxes, so it gives INTERSECTS (inner=False)
        and WITHIN (inner=True) natively. CENTRE is not a box test at all,
        so it is computed here by testing the centre of each boundary box -
        the ones the two native modes disagree about.

        Not a part of the interface provided by VectorIndexer.
        """
        if self.mode == const.ContainmentMode.INTERSECTS:
            return polygon_to_geohashes(polygon, level, inner=False)
        inner: set[str] = polygon_to_geohashes(polygon, level, inner=True)
        if self.mode == const.ContainmentMode.WITHIN:
            return inner
        outer: set[str] = polygon_to_geohashes(polygon, level, inner=False)
        edge: set[str] = {
            h
            for h in (outer - inner)  # All edge cells
            if Point(*reversed(decode(h))).within(polygon)
        }  # Edge cells with a centre point within the polygon
        return edge | inner

    @staticmethod
    def cell_to_point(cell: str) -> Point:
        lat, lon, _, _ = decode_exactly(cell)
        return Point(lon, lat)

    def cells_to_polygons(self, cells: Iterable[str]) -> Iterable[Polygon]:
        """
        Batched construction, which dominates the per-cell cost of the
        line cover. A geohash cell is a longitude/latitude box, so the
        whole set is one vectorised shapely.box call.
        """
        centres = np.array([decode_exactly(c)[:4] for c in cells])
        lat, lon, lat_err, lon_err = centres.T
        return shapely.box(lon - lon_err, lat - lat_err, lon + lon_err, lat + lat_err)

    @staticmethod
    def cell_to_polygon(cell: str) -> Polygon:
        lat, lon, lat_err, lon_err = decode_exactly(cell)
        return box(
            lon - lon_err,
            lat - lat_err,
            lon + lon_err,
            lat + lat_err,
        )
