import sys
import click
import click_log
import tempfile
import pyproj
from math import ceil

from s2geometry import pywraps2 as S2

import pandas as pd
import geopandas as gpd
from shapely.geometry import box, Polygon, LineString, Point
from shapely.ops import transform
from pyproj import CRS, Transformer

from typing import Union
from pathlib import Path

import vector2dggs.constants as const
import vector2dggs.common as common

from vector2dggs import __version__


def s2_secondary_index(df: pd.DataFrame, parent_level: int) -> pd.DataFrame:
    # NB also converts the index to S2 cell tokens
    index_series = df.index.to_series().astype(object)
    df[f"s2_{parent_level:02}"] = index_series.map(
        lambda cell_id: cell_id.parent(parent_level).ToToken()
    )
    df.index = index_series.map(lambda cell_id: cell_id.ToToken())
    return df


def bbox_area_in_m2(
    geom: Polygon,
    src_crs: Union[str, CRS] = "EPSG:4326",
    dst_crs: Union[str, CRS] = "EPSG:6933",
) -> float:
    """
    Calculate the area of the bounding box of a geometry in square meters.
    """
    minx, miny, maxx, maxy = geom.bounds
    bbox = box(minx, miny, maxx, maxy)
    transformer = Transformer.from_crs(src_crs, dst_crs, always_xy=True)
    projected_bbox = transform(transformer.transform, bbox)
    return projected_bbox.area


def max_cells_for_geom(
    geom: Union[Polygon, LineString], level: int, margin: float = 1.02
) -> int:
    """
    Calculate the maximum number of S2 cells that are appropriate for the given geometry and level.
    This is based on the area of the geometry's bounding box,
    and the maximum area of S2 cells at the given level.
    """
    area = bbox_area_in_m2(geom)
    max_cells = ceil(max(1, area / const.S2_CELLS_MAX_AREA_M2_BY_LEVEL[level]))
    return ceil(max_cells * margin)


def cell_center_is_inside_polygon(cell: S2.S2CellId, polygon: S2.S2Polygon) -> bool:
    """Determines if the center of the S2 cell is inside the polygon"""
    cell_center = S2.S2Cell(cell).GetCenter()
    return polygon.Contains(cell_center)


def s2_polyfill_polygons(df: gpd.GeoDataFrame, level: int) -> gpd.GeoDataFrame:

    def generate_s2_covering(
        geom: Polygon, level: int, centroid_inside: bool = True
    ) -> set[S2.S2CellId]:
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
        coverer = S2.S2RegionCoverer()

        max_cells = max_cells_for_geom(geom, level)
        coverer.set_max_cells(max_cells)
        coverer.set_min_level(level)
        coverer.set_max_level(level)

        covering: list[S2.S2CellId] = coverer.GetCovering(s2polygon)

        if centroid_inside:
            # Coverings are "intersects" modality, polyfill is "centre inside" modality
            # ergo, filter out covering cells that are not inside the polygon
            covering = {
                cell
                for cell in covering
                if cell_center_is_inside_polygon(cell, s2polygon)
            }
        else:
            set(covering)

        return covering

    df["s2index"] = df["geometry"].apply(lambda geom: generate_s2_covering(geom, level))
    df = df[
        df["s2index"].map(lambda x: len(x) > 0)
    ]  # Remove rows with no covering at this level

    return df


def s2_cell_ids_from_linestring(
    linestring: LineString, level: int
) -> list[S2.S2CellId]:
    latlngs = [S2.S2LatLng.FromDegrees(lat, lon) for lon, lat in linestring.coords]
    polyline = S2.S2Polyline(latlngs)

    coverer = S2.S2RegionCoverer()
    max_cells = max_cells_for_geom(linestring, level)
    coverer.set_max_cells(max_cells)
    coverer.set_min_level(level)
    coverer.set_max_level(level)

    return coverer.GetCovering(polyline)


def s2_cell_id_from_point(geom: Point, level: int) -> S2.S2CellId:
    """
    Convert a point geometry to an S2 cell at the specified level.
    """
    latlng = S2.S2LatLng.FromDegrees(geom.y, geom.x)
    return S2.S2CellId(latlng).parent(level)


def s2_polyfill(df: gpd.GeoDataFrame, level: int) -> pd.DataFrame:

    df_polygon = df[df.geom_type == "Polygon"].copy()
    if len(df_polygon.index) > 0:
        df_polygon = (
            s2_polyfill_polygons(df_polygon, level)
            .explode("s2index")
            .set_index("s2index")
        )

    df_linestring = df[df.geom_type == "LineString"].copy()
    if len(df_linestring.index) > 0:
        df_linestring["s2index"] = df_linestring.geometry.apply(
            lambda geom: s2_cell_ids_from_linestring(geom, level)
        )
        df_linestring = df_linestring.explode("s2index").set_index("s2index")

    df_point = df[df.geom_type == "Point"].copy()
    if len(df_point.index) > 0:
        df_point["s2index"] = df_point.geometry.apply(
            lambda geom: s2_cell_id_from_point(geom, level)
        )
        df_point = df_point.set_index("s2index")

    return pd.concat(
        map(
            lambda _df: pd.DataFrame(_df.drop(columns=[_df.geometry.name])),
            [df_polygon, df_linestring, df_point],
        )
    )


def compact_tokens(tokens: set[str]) -> set[str]:
    """
    Compact a set of S2 DGGS cells.
    Cells must be at the same resolution.
    """
    cell_ids: list[S2.S2CellId] = [
        S2.S2CellId.FromToken(token, len(token)) for token in tokens
    ]
    cell_union: S2.S2CellUnion = S2.S2CellUnion(
        cell_ids
    )  # Vector of sorted, non-overlapping S2CellId
    cell_union.NormalizeS2CellUnion()  # Mutates; 'normalize' == 'compact'
    return {c.ToToken() for c in cell_union.cell_ids()}


def token_to_child_token(token: str, level: int) -> str:
    """
    Returns first child (as string token) of a cell (also represented as a string
    token) at a specific level.
    """
    cell: S2.S2CellId = S2.S2CellId.FromToken(token, len(token))
    if level <= cell.level():
        raise ValueError("Level must be greater than the current level of the cell.")
    # Get the child cell iterator
    return cell.child_begin(level).ToToken()


def s2_compaction(
    df: pd.DataFrame,
    res: int,
    col_order: list,
    dggs_col: str,
    id_field: str,
) -> pd.DataFrame:
    """
    Compacts an S2 dataframe up to a given low resolution (parent_res), from an existing maximum resolution (res).
    """
    return common.compaction(
        df,
        res,
        id_field,
        col_order,
        dggs_col,
        compact_tokens,
        token_to_child_token,
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
    type=click.Choice(list(map(str, range(const.MIN_S2, const.MAX_S2 + 1)))),
    help="S2 level to index",
    nargs=1,
)
@click.option(
    "-pr",
    "--parent_res",
    "parent_level",
    required=False,
    type=click.Choice(list(map(str, range(const.MIN_S2, const.MAX_S2 + 1)))),
    help="S2 parent level for the output partition. Defaults to resolution - 6",
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
    help="Retain attributes in output. The default is to create an output that only includes S2 cell ID and the ID given by the -id field (or the default index ID).",
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
def s2(
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
    Ingest a vector dataset and index it to the S2 DGGS.

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
            "s2",
            s2_polyfill,
            s2_secondary_index,
            s2_compaction if compact else None,
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
