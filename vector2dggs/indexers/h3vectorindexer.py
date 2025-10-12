"""

@author: ndemaio
"""
import h3 as h3py
import h3pandas  # Necessary import despite lack of explicit use

import pandas as pd
import geopandas as gpd

from vector2dggs.indexers.vectorindexer import VectorIndexer

class H3VectorIndexer(VectorIndexer):
    """
    Provides integration for Uber's H3 DGGS.
    """
    
    def polyfill(self, df: gpd.GeoDataFrame, resolution: int) -> pd.DataFrame:
        """
        Implementation of abstract function.
        """
        
        df_polygon = df[df.geom_type == "Polygon"]
        if not df_polygon.empty:
            df_polygon = df_polygon.h3.polyfill_resample(
                resolution, return_geometry=False
            ).drop(columns=["index"])

        df_linestring = df[df.geom_type == "LineString"]
        if len(df_linestring.index) > 0:
            df_linestring = (
                df_linestring.h3.linetrace(resolution)
                .explode("h3_linetrace")
                .set_index("h3_linetrace")
            )
            df_linestring = df_linestring[~df_linestring.index.duplicated(keep="first")]

        df_point = df[df.geom_type == "Point"]
        if len(df_point.index) > 0:
            df_point = df_point.h3.geo_to_h3(resolution, set_index=True)

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
        
        return df.h3.h3_to_parent(parent_res)
        
    def compaction(
        self,
        df: pd.DataFrame,
        res: int,
        col_order: list,
        dggs_col: str,
        id_field: str,
    ) -> pd.DataFrame:
        """
        Compacts an H3 dataframe up to a given low resolution (parent_res),
        from an existing maximum resolution (res).
        
        Implementation of abstract function.
        """
        return self.compaction_common(
            df,
            res,
            id_field,
            col_order,
            dggs_col,
            h3py.compact_cells,
            h3py.cell_to_center_child,
        )

