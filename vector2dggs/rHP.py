import sys
import click
import click_log
import tempfile
import pyproj

import rhppandas  # Necessary import despite lack of explicit use

import pandas as pd
import geopandas as gpd

from typing import Union
from pathlib import Path
from rhealpixdggs.conversion import compress_order_cells
from rhppandas.util.const import COLUMNS

# from rhealpixdggs.rhp_wrappers import rhp_to_center_child, rhp_is_valid
from rhealpixdggs.rhp_wrappers import rhp_is_valid
from rhealpixdggs.dggs import RHEALPixDGGS
from rhealpixdggs.dggs import WGS84_003

import vector2dggs.constants as const
import vector2dggs.common as common

from vector2dggs import __version__


def rhp_secondary_index(df: pd.date_range, parent_res: int) -> pd.DataFrame:
    return df.rhp.rhp_to_parent(parent_res)


def rhppolyfill(df: gpd.GeoDataFrame, resolution: int) -> pd.DataFrame:
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


# TODO replace when merged https://github.com/manaakiwhenua/rhealpixdggs-py/pull/37
def rhp_to_center_child(
    rhpindex: str, res: int = None, dggs: RHEALPixDGGS = WGS84_003
) -> str:
    """
    Returns central child of rhpindex at resolution res (immediate central
    child if res == None).

    Returns None if the cell index is invalid.

    Returns None if the DGGS has an even number of cells on a side.

    EXAMPLES::

        >>> rhp_to_center_child('S001450634')
        'S0014506344'
        >>> rhp_to_center_child('S001450634', res=13)
        'S001450634444'
        >>> rhp_to_center_child('INVALID')
    """
    # Stop early if the cell index is invalid
    if not rhp_is_valid(rhpindex, dggs):
        return None

    # DGGSs with even numbers of cells on a side never have a cell at the centre
    if (dggs.N_side % 2) == 0:
        return None

    # Handle mismatch between cell resolution and requested child resolution
    parent_res = len(rhpindex) - 1
    if res is not None and res < parent_res:
        return rhpindex

    # Standard case (including parent_res == res)
    else:
        # res == None returns the central child from one level down (by convention)
        added_levels = 1 if res is None else res - parent_res

        # Derive index of centre child and append that to rhpindex
        # NOTE: only works for odd values of N_side
        c_index = int((dggs.N_side**2 - 1) / 2)

        # Append the required number of child digits to cell index
        child_index = rhpindex + "".join(str(c_index) for _ in range(0, added_levels))

        return child_index


def compact_cells(cells: set[str]) -> set[str]:
    """
    Compact a set of rHEALPix DGGS cells.
    Cells must be at the same resolution.
    See https://github.com/manaakiwhenua/rhealpixdggs-py/issues/35#issuecomment-3186073554
    """
    previous_result = set(cells)
    while True:
        current_result = set(compress_order_cells(previous_result))
        if previous_result == current_result:
            break
        previous_result = current_result
    return previous_result


def rhpcompaction(
    df: pd.DataFrame,
    res: int,
    col_order: list,
    dggs_col: str,
    id_field: str,
) -> pd.DataFrame:
    """
    Compacts an rHP dataframe up to a given low resolution (parent_res), from an existing maximum resolution (res).
    """
    return common.compaction(
        df,
        res,
        id_field,
        col_order,
        dggs_col,
        compact_cells,
        rhp_to_center_child,
    )


@click.command(context_settings={"show_default": True})
@click_log.simple_verbosity_option(common.LOGGER)
@click.argument("vector_input", required=True, type=click.Path(), nargs=1)
@click.argument("output_directory", required=True, type=click.Path(), nargs=1)
@click.option(
    "-r",
    "--resolution",
    required=True,
    type=click.Choice(list(map(str, range(const.MIN_RHP, const.MAX_RHP + 1)))),
    help="rHEALPix resolution to index",
    nargs=1,
)
@click.option(
    "-pr",
    "--parent_res",
    required=False,
    type=click.Choice(list(map(str, range(const.MIN_RHP, const.MAX_RHP + 1)))),
    help="rHEALPix Parent resolution for the output partition. Defaults to resolution - 6",
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
    help="Retain attributes in output. The default is to create an output that only includes rHEALPix cell ID and the ID given by the -id field (or the default index ID).",
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
    type=click.Choice(const.SPATIAL_SORTING_METHODS),
    default=const.DEFAULTS["s"],
    help="Spatial sorting method when perfoming spatial partitioning.",
)
@click.option(
    "-crs",
    "--cut_crs",
    required=False,
    default=const.DEFAULTS["crs"],
    type=int,
    help="Set the coordinate reference system (CRS) used for cutting large geometries (see `--cut_threshold`). Defaults to the same CRS as the input. Should be a valid EPSG code.",
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
    "-cp",
    "--compression",
    required=False,
    default=const.DEFAULTS["cp"],
    type=str,
    help="Compression method to use for the output Parquet files. Options include 'snappy', 'gzip', 'brotli', 'lz4', 'zstd', etc. Use 'none' for no compression.",
    nargs=1,
)
@click.option(
    "-lyr",
    "--layer",
    required=False,
    default=const.DEFAULTS["lyr"],
    type=str,
    help="Name of the layer or table to read when using an input that supports layers or tables",
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
@click.option(
    "-co",
    "--compact",
    is_flag=True,
    help="Compact the rHEALPix cells up to the parent resolution. Compaction requires an id_field.",
)
@click.option("-o", "--overwrite", is_flag=True)
@click.version_option(version=__version__)
def rhp(
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
    compression: str,
    layer: str,
    geom_col: str,
    tempdir: Union[str, Path],
    compact: bool,
    overwrite: bool,
):
    """
    Ingest a vector dataset and index it to the rHEALPix DGGS.

    VECTOR_INPUT is the path to input vector geospatial data.
    OUTPUT_DIRECTORY should be a directory, not a file or database table, as it will instead be the write location for an Apache Parquet data store.
    """
    tempfile.tempdir = tempdir if tempdir is not None else tempfile.tempdir

    common.check_resolutions(resolution, parent_res)
    common.check_compaction_requirements(compact, id_field)

    con, vector_input = common.db_conn_and_input_path(vector_input)
    output_directory = common.resolve_output_path(output_directory, overwrite)

    if cut_crs is not None:
        cut_crs = pyproj.CRS.from_user_input(cut_crs)

    try:
        common.index(
            "rhp",
            rhppolyfill,
            rhp_secondary_index,
            rhpcompaction if compact else None,
            vector_input,
            output_directory,
            int(resolution),
            parent_res,
            keep_attributes,
            chunksize,
            spatial_sorting,
            cut_threshold,
            threads,
            compression=compression,
            cut_crs=cut_crs,
            id_field=id_field,
            con=con,
            layer=layer,
            geom_col=geom_col,
            overwrite=overwrite,
        )
    except:
        raise
    else:
        sys.exit(0)
