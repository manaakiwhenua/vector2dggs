import sys
import click
import click_log
import tempfile
import pyproj

from s2geometry import pywraps2 as S2

import pandas as pd
import geopandas as gpd

from typing import Union
from pathlib import Path

import vector2dggs.constants as const
import vector2dggs.common as common

from vector2dggs import __version__


def s2_secondary_index(df: gpd.GeoDataFrame, parent_res: int) -> gpd.GeoDataFrame:
    # NB also converts the index to S2 cell tokens
    index_series = df.index.to_series()
    df[f's2_{parent_res:02}'] = index_series.map(lambda cell: cell.parent(parent_res).ToToken())
    df.index = index_series.map(lambda cell: cell.ToToken())
    return df

def cell_center_is_inside_polygon(cell: S2.S2Cell, polygon: S2.S2Polygon) -> bool:
    '''Determines if the center of the S2 cell is inside the polygon'''
    cell_center = S2.S2Cell(cell).GetCenter()
    return polygon.Contains(cell_center)

def s2_polyfill_polygons(df: gpd.GeoDataFrame, level: int) -> gpd.GeoDataFrame:
    df = df[df.geom_type == "Polygon"].copy()
    records = []

    def generate_s2_covering(geom, level, centroid_inside=True):
        # Prepare loops: first the exterior loop, then the interior loops
        loops = []
        # Exterior ring
        latlngs = [
            S2.S2LatLng.FromDegrees(lat, lon)
            for lon, lat in geom.exterior.coords
        ]
        s2loop = S2.S2Loop([latlng.ToPoint() for latlng in latlngs])
        s2loop.Normalize() # Ensure the exterior is oriented counter-clockwise
        loops.append(s2loop)

        # Interior rings (polygon holes)
        for interior in geom.interiors:
            interior_latlngs = [
                S2.S2LatLng.FromDegrees(lat, lon)
                for lon, lat in interior.coords
            ]
            s2interior_loop = S2.S2Loop([latlng.ToPoint() for latlng in interior_latlngs])
            s2interior_loop.Normalize()  # Ensure interior holes are oriented clockwise
            loops.append(s2interior_loop)
        
        # Build an S2Polygon from the loops
        s2polygon = S2.S2Polygon()
        s2polygon.InitNested(loops)
        
        # Use S2RegionCoverer to get the cell IDs at the specified resolution
        coverer = S2.S2RegionCoverer()
        
        # TODO experiment with using a compressed representation of the polygon and exploding it rather than using the same level for min and max
        coverer.set_max_cells(5000)  # TODO parameterize this?
        coverer.set_min_level(level)
        coverer.set_max_level(level)

        covering : set[S2.Cell] = coverer.GetCovering(s2polygon)

        if centroid_inside:
            # Coverings are "intersects" modality, polyfill is "centre inside" modality
            # ergo, filter out covering cells that are not inside the polygon
            covering = {cell for cell in covering if cell_center_is_inside_polygon(cell, s2polygon)}
        
        return covering


    df['s2index'] = df['geometry'].apply(lambda geom: generate_s2_covering(geom, level))
    df = df[df['s2index'].map(lambda x: len(x) > 0)] # Remove rows with no covering at this resolution

    return df

def s2_polyfill(df: gpd.GeoDataFrame, resolution: int):

    df_polygon = s2_polyfill_polygons(df, resolution).explode("s2index").set_index("s2index")

    # TODO linestrings
    # TODO points
    
    return pd.concat(
        map(
            lambda _df: pd.DataFrame(_df.drop(columns=[_df.geometry.name])),
            [df_polygon],
        )
    )


@click.command(context_settings={"show_default": True})
@click_log.simple_verbosity_option(common.LOGGER)
@click.argument("vector_input", required=True, type=click.Path(), nargs=1)
@click.argument("output_directory", required=True, type=click.Path(), nargs=1)
@click.option(
    "-r",
    "--resolution",
    required=True,
    type=click.Choice(list(map(str, range(const.MIN_S2, const.MAX_S2 + 1)))),
    help="H3 resolution to index",
    nargs=1,
)
@click.option(
    "-pr",
    "--parent_res",
    required=False,
    type=click.Choice(list(map(str, range(const.MIN_S2, const.MAX_S2 + 1)))),
    help="H3 Parent resolution for the output partition. Defaults to resolution - 6",
)
@click.option(
    "-id",
    "--id_field",
    required=False,
    default=const.DEFAULTS["id"],
    type=str,
    help="Field to use as an ID; defaults to a constructed single 0...n index on the original feature order.",
    nargs=1,
)
@click.option(
    "-k",
    "--keep_attributes",
    is_flag=True,
    show_default=True,
    default=const.DEFAULTS["k"],
    help="Retain attributes in output. The default is to create an output that only includes H3 cell ID and the ID given by the -id field (or the default index ID).",
)
@click.option(
    "-ch",
    "--chunksize",
    required=True,
    type=int,
    default=const.DEFAULTS["ch"],
    help="The number of rows per index partition to use when spatially partioning. Adjusting this number will trade off memory use and time.",
    nargs=1,
)
@click.option(
    "-s",
    "--spatial_sorting",
    type=click.Choice(["hilbert", "morton", "geohash"]),
    default=const.DEFAULTS["s"],
    help="Spatial sorting method when perfoming spatial partitioning.",
)
@click.option(
    "-crs",
    "--cut_crs",
    required=False,
    default=const.DEFAULTS["crs"],
    type=int,
    help="Set the coordinate reference system (CRS) used for cutting large geometries (see `--cur-threshold`). Defaults to the same CRS as the input. Should be a valid EPSG code.",
    nargs=1,
)
@click.option(
    "-c",
    "--cut_threshold",
    required=True,
    default=const.DEFAULTS["c"],
    type=int,
    help="Cutting up large geometries into smaller geometries based on a target length. Units are assumed to match the input CRS units unless the `--cut_crs` is also given, in which case units match the units of the supplied CRS.",
    nargs=1,
)
@click.option(
    "-t",
    "--threads",
    required=False,
    default=const.DEFAULTS["t"],
    type=int,
    help="Amount of threads used for operation",
    nargs=1,
)
@click.option(
    "-tbl",
    "--table",
    required=False,
    default=const.DEFAULTS["tbl"],
    type=str,
    help="Name of the table to read when using a spatial database connection as input",
    nargs=1,
)
@click.option(
    "-g",
    "--geom_col",
    required=False,
    default=const.DEFAULTS["g"],
    type=str,
    help="Column name to use when using a spatial database connection as input",
    nargs=1,
)
@click.option(
    "--tempdir",
    default=const.DEFAULTS["tempdir"],
    type=click.Path(),
    help="Temporary data is created during the execution of this program. This parameter allows you to control where this data will be written.",
)
@click.option("-o", "--overwrite", is_flag=True)
@click.version_option(version=__version__)
def s2(
    vector_input: Union[str, Path],
    output_directory: Union[str, Path],
    resolution: str,
    parent_res: str,
    id_field: str,
    keep_attributes: bool,
    chunksize: int,
    spatial_sorting: str,
    cut_crs: int,
    cut_threshold: int,
    threads: int,
    table: str,
    geom_col: str,
    tempdir: Union[str, Path],
    overwrite: bool,
):
    """
    Ingest a vector dataset and index it to the S2 DGGS.

    VECTOR_INPUT is the path to input vector geospatial data.
    OUTPUT_DIRECTORY should be a directory, not a file or database table, as it will instead be the write location for an Apache Parquet data store.
    """
    tempfile.tempdir = tempdir if tempdir is not None else tempfile.tempdir

    common.check_resolutions(resolution, parent_res)

    con, vector_input = common.db_conn_and_input_path(vector_input)
    output_directory = common.resolve_output_path(output_directory, overwrite)

    if cut_crs is not None:
        cut_crs = pyproj.CRS.from_user_input(cut_crs)

    try:
        common.index(
            "s2",
            s2_polyfill,
            s2_secondary_index,
            vector_input,
            output_directory,
            int(resolution),
            parent_res,
            keep_attributes,
            chunksize,
            spatial_sorting,
            cut_threshold,
            threads,
            cut_crs=cut_crs,
            id_field=id_field,
            con=con,
            table=table,
            geom_col=geom_col,
            overwrite=overwrite,
        )
    except:
        raise
    else:
        sys.exit(0)
