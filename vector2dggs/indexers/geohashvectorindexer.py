from collections.abc import Iterable
from itertools import product

import geopandas as gpd
import pandas as pd
from geohash import decode, decode_exactly, encode  # python-geohash
from geohash_polygon import polygon_to_geohashes  # rusty-polygon-geohasher
from shapely.geometry import Point, Polygon, box

from vector2dggs.indexers.geohash import traversal as geohash_traversal
from vector2dggs.indexers.vectorindexer import VectorIndexer


class GeohashVectorIndexer(VectorIndexer):
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
                        lambda geom: geohash_traversal.linetrace_linewise(geom, level)
                    )
                }
            )
            .drop(columns=[geom_col])
            .explode(gh_col, ignore_index=True)
            .dropna(subset=[gh_col])
            .set_index(gh_col)
        )
        return pd.DataFrame(result[~result.index.duplicated(keep="first")])

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

    def gh_children(self, geohash: str, desired_resolution: int) -> int:
        """
        Determine the number of children in the geohash refinement, determined by
        the additional character levels.

        Not a part of the interface provided by VectorIndexer.
        """
        current_resolution = len(geohash)
        additional_length = desired_resolution - current_resolution
        return 32**additional_length  # Each new character increases resolution by 32

    def get_central_child(self, geohash: str, precision: int):
        """
        Return an approximate central child of the geohash.
        NB if only an arbitrary child is needed, use get_child_geohash

        Not a part of the interface provided by VectorIndexer.
        """
        lat, lon = decode(geohash)
        return encode(lat, lon, precision=precision)

    def _polygon_to_geohashes(self, polygon: Polygon, level: int) -> set[str]:
        """
        Function to compute geohash set for one polygon geometry

        NB this implements a point-inside hash, but geohash_polygon only
        supports "within" or "intersects" (on the basis of geohashes as
        _polygon_ geometries) which means we have to perform additional
        computation to support "polyfill" as defined by H3.

        A future version of vector2dggs may support within/intersects modality,
        at which point that would just be outer/inner with no further
        computation.

        Not a part of the interface provided by VectorIndexer.
        """
        outer: set[str] = polygon_to_geohashes(polygon, level, inner=False)
        inner: set[str] = polygon_to_geohashes(polygon, level, inner=True)
        edge: set[str] = {
            h
            for h in (outer - inner)  # All edge cells
            if Point(*reversed(decode(h))).within(polygon)
        }  # Edge cells with a center within the polygon
        return edge | inner

    @staticmethod
    def cell_to_point(cell: str) -> Point:
        lat, lon, _, _ = decode_exactly(cell)
        return Point(lon, lat)

    @staticmethod
    def cell_to_polygon(cell: str) -> Polygon:
        lat, lon, lat_err, lon_err = decode_exactly(cell)
        return box(
            lon - lon_err,
            lat - lat_err,
            lon + lon_err,
            lat + lat_err,
        )
