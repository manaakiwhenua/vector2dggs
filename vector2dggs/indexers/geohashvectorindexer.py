"""

@author: ndemaio
"""

from geohash_polygon import polygon_to_geohashes  # rusty-polygon-geohasher
from geohash import encode, decode  # python-geohash

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon

from vector2dggs.indexers.vectorindexer import VectorIndexer


class GeohashVectorIndexer(VectorIndexer):
    """
    Provides integration for the Geohash geocode system.
    """

    def __init__(self, dggs):
        super().__init__(dggs=dggs)
        self.GEOHASH_BASE32_SET = set("0123456789bcdefghjkmnpqrstuvwxyz")

    def polyfill(self, df: gpd.GeoDataFrame, level: int) -> pd.DataFrame:
        """
        Implementation of abstract function.
        """

        gh_col = "geohash"
        df_polygon = df[df.geom_type == "Polygon"].copy()
        if not df_polygon.empty:
            df_polygon = (
                df_polygon.assign(
                    **{
                        gh_col: df_polygon.geometry.apply(
                            lambda geom: self._polygon_to_geohashes(geom, level)
                        )
                    }
                )
                .explode(gh_col, ignore_index=True)
                .set_index(gh_col)
            )

        # TODO linestring support
        # e.g. JS implementation https://github.com/alrico88/geohashes-along

        df_point = df[df.geom_type == "Point"].copy()
        if len(df_point.index) > 0:
            df_point[gh_col] = df_point.geometry.apply(
                lambda geom: encode(geom.y, geom.x, precision=level)
            )
            df_point = df_point.set_index(gh_col)

        return pd.concat(
            map(
                lambda _df: pd.DataFrame(_df.drop(columns=[_df.geometry.name])),
                [df_polygon, df_point],
            )
        )

    def secondary_index(self, df: pd.DataFrame, parent_level: int) -> pd.DataFrame:
        """
        Implementation of abstract function.
        """

        df[f"geohash_{parent_level:02}"] = df.index.to_series().str[:parent_level]
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
        Compacts a geohash dataframe up to a given low resolution (parent_res),
        from an existing maximum resolution (res).

        Implementation of abstract function.
        """
        return self.compaction_common(
            df, res, id_field, col_order, dggs_col, self.compact, self.get_child_geohash
        )

    def compact(self, cells: set[str]) -> set[str]:
        """
        Compact a set of geohash cells.
        Cells must be at the same resolution.

        Not a part of the interface provided by VectorIndexer.
        """
        current_set = set(cells)
        while True:
            parent_map = {}
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
