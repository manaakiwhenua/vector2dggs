import sys
import click
import click_log
import tempfile
import pyproj

from geohash_polygon import polygon_to_geohashes  # rusty-polygon-geohasher
from geohash import encode, decode  # python-geohash

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon

from typing import Union
from pathlib import Path

import vector2dggs.constants as const
import vector2dggs.common as common

from vector2dggs import __version__

GEOHASH_BASE32_SET = set("0123456789bcdefghjkmnpqrstuvwxyz")


def gh_secondary_index(df: pd.DataFrame, parent_level: int) -> pd.DataFrame:
    df[f"geohash_{parent_level:02}"] = df.index.to_series().str[:parent_level]
    return df


# NB this implements a point-inside hash, but geohash_polygon only supports "within" or "intersects" (on the basis of geohashes as _polygon_ geometries) which means we have to perform additional computation to support "polyfill" as defined by H3
# A future version of vector2dggs may support within/intersects modality, at which point that would just be outer/inner with no further computation
def _polygon_to_geohashes(polygon: Polygon, level: int) -> set[str]:
    # Function to compute geohash set for one polygon geometry
    outer: set[str] = polygon_to_geohashes(polygon, level, inner=False)
    inner: set[str] = polygon_to_geohashes(polygon, level, inner=True)
    edge: set[str] = {
        h
        for h in (outer - inner)  # All edge cells
        if Point(*reversed(decode(h))).within(polygon)
    }  # Edge cells with a center within the polygon
    return edge | inner


def gh_polyfill(df: gpd.GeoDataFrame, level: int) -> pd.DataFrame:
    gh_col = f"geohash"
    df_polygon = df[df.geom_type == "Polygon"].copy()
    if not df_polygon.empty:
        df_polygon = (
            df_polygon.assign(
                **{
                    gh_col: df_polygon.geometry.apply(
                        lambda geom: _polygon_to_geohashes(geom, level)
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


def gh_children(geohash: str, desired_resolution: int) -> int:
    """
    Determine the number of children in the geohash refinement, determined by the additional character levels.
    """
    current_resolution = len(geohash)
    additional_length = desired_resolution - current_resolution
    return 32**additional_length  # Each new character increases resolution by 32


def compact(cells: set[str]) -> set[str]:
    """
    Compact a set of geohash cells.
    Cells must be at the same resolution.
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


def get_central_child(geohash: str, precision: int):
    """
    Return an approximate central child of the geohash.
    NB if only an arbitrary child is needed, use get_child_geohash
    """
    lat, lon = decode(geohash)
    return encode(lat, lon, precision=precision)


def get_child_geohash(geohash: str, desired_length: int, child: str = "0"):
    """
    Get a child geohash of the specified length by extending the input geohash.
    Child geohash is
    """
    if child not in GEOHASH_BASE32_SET:
        raise ValueError(
            f"Invalid child character '{child}'. Must be one of {''.join(GEOHASH_BASE32_SET)}."
        )

    if len(geohash) >= desired_length:
        return geohash
    return geohash.ljust(desired_length, child)


def gh_compaction(
    df: pd.DataFrame,
    res: int,
    col_order: list,
    dggs_col: str,
    id_field: str,
) -> pd.DataFrame:
    """
    Compacts a geohash dataframe up to a given low resolution (parent_res), from an existing maximum resolution (res).
    """
    return common.compaction(
        df, res, id_field, col_order, dggs_col, compact, get_child_geohash
    )


@click.command(context_settings={"show_default": True})
@click_log.simple_verbosity_option(common.LOGGER)
@click.argument("vector_input", required=True, type=click.Path(), nargs=1)
@click.argument("output_directory", required=True, type=click.Path(), nargs=1)
@click.option(
    "-r",
    "--resolution",
    "level",
    required=True,
    type=click.Choice(list(map(str, range(const.MIN_GEOHASH, const.MAX_GEOHASH + 1)))),
    help="Geohash level to index",
    nargs=1,
)
@click.option(
    "-pr",
    "--parent_res",
    "parent_level",
    required=False,
    type=click.Choice(list(map(str, range(const.MIN_GEOHASH, const.MAX_GEOHASH + 1)))),
    help="Geohash parent level for the output partition. Defaults to resolution - 6",
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
    help="Retain attributes in output. The default is to create an output that only includes Geohash cell ID and the ID given by the -id field (or the default index ID).",
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
    help="Compact the geohash cells up to the parent resolution. Compaction requires an id_field.",
)
@click.option("-o", "--overwrite", is_flag=True)
@click.version_option(version=__version__)
def geohash(
    vector_input: Union[str, Path],
    output_directory: Union[str, Path],
    level: str,
    parent_level: str,
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
    Ingest a vector dataset and index it using the Geohash geocode system.

    VECTOR_INPUT is the path to input vector geospatial data.
    OUTPUT_DIRECTORY should be a directory, not a file or database table, as it will instead be the write location for an Apache Parquet data store.
    """
    tempfile.tempdir = tempdir if tempdir is not None else tempfile.tempdir

    common.check_resolutions(level, parent_level)

    con, vector_input = common.db_conn_and_input_path(vector_input)
    output_directory = common.resolve_output_path(output_directory, overwrite)

    if cut_crs is not None:
        cut_crs = pyproj.CRS.from_user_input(cut_crs)

    try:
        common.index(
            "geohash",
            gh_polyfill,
            gh_secondary_index,
            gh_compaction if compact else None,
            vector_input,
            output_directory,
            int(level),
            parent_level,
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
