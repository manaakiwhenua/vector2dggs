"""

@author: ndemaio
"""

from rhealpixdggs.conversion import compress_order_cells
from rhealpixdggs.rhp_wrappers import rhp_to_center_child
from rhppandas.util.const import COLUMNS

import rhppandas  # Necessary import despite lack of explicit use

import pandas as pd
import geopandas as gpd

from vector2dggs.indexers.vectorindexer import VectorIndexer


class RHPVectorIndexer(VectorIndexer):
    """
    Provides integration for MWLR's rHEALPix DGGS.
    """

    def polyfill(self, df: gpd.GeoDataFrame, resolution: int) -> pd.DataFrame:
        """
        Implementation of abstract function.
        """

        df_polygon = df[df.geom_type == "Polygon"]
        if len(df_polygon.index) > 0:
            df_polygon = df_polygon.rhp.polyfill_resample(
                resolution, return_geometry=False, compress=False
            ).drop(columns=["index"])

        df_linestring = df[df.geom_type == "LineString"]
        if len(df_linestring.index) > 0:
            df_linestring = (
                df_linestring.rhp.linetrace(resolution)
                .explode(COLUMNS["linetrace"])
                .set_index(COLUMNS["linetrace"])
            )
            df_linestring = df_linestring[~df_linestring.index.duplicated(keep="first")]

        df_point = df[df.geom_type == "Point"]
        if len(df_point.index) > 0:
            df_point = df_point.rhp.geo_to_rhp(resolution, set_index=True)

        return pd.concat(
            map(
                lambda _df: pd.DataFrame(_df.drop(columns=[_df.geometry.name])),
                [df_polygon, df_linestring, df_point],
            )
        )

    def secondary_index(self, df: pd.DataFrame, parent_res: int) -> pd.DataFrame:
        """
        Implementation of abstract function.
        """

        return df.rhp.rhp_to_parent(parent_res)

    def compaction(
        self,
        df: pd.DataFrame,
        res: int,
        col_order: list,
        dggs_col: str,
        id_field: str,
    ) -> pd.DataFrame:
        """
        Compacts an rHP dataframe up to a given low resolution (parent_res),
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
            rhp_to_center_child,
        )

    def compact_cells(self, cells: set[str]) -> set[str]:
        """
        Compact a set of rHEALPix DGGS cells.
        Cells must be at the same resolution.
        See https://github.com/manaakiwhenua/rhealpixdggs-py/issues/35#issuecomment-3186073554

        Not a part of the interface provided by VectorIndexer.
        """
        previous_result = set(cells)
        while True:
            current_result = set(compress_order_cells(previous_result))
            if previous_result == current_result:
                break
            previous_result = current_result
        return previous_result
