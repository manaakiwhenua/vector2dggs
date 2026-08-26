import tempfile
from pathlib import Path

import click
import click_log
import pyproj

import vector2dggs.common as common
import vector2dggs.constants as const
from vector2dggs import __version__


def make_dggs_command(
    dggs_key: str,
    command_name: str,
    display_name: str,
    min_res: int,
    max_res: int,
) -> click.Command:
    res_choices = list(map(str, range(min_res, max_res + 1)))

    @click.command(name=command_name, context_settings={"show_default": True})
    @click_log.simple_verbosity_option(common.LOGGER)
    @click.argument("vector_input", required=True, type=click.Path(), nargs=1)
    @click.argument("output_directory", required=True, type=click.Path(), nargs=1)
    @click.option(
        "-r",
        "--resolution",
        required=True,
        type=click.Choice(res_choices),
        help=f"{display_name} resolution to index",
        nargs=1,
    )
    @click.option(
        "-pr",
        "--parent_res",
        required=False,
        type=click.Choice(res_choices),
        help=f"{display_name} parent resolution for the output partition. Defaults to resolution - 6",
    )
    @click.option(
        "-id",
        "--id_field",
        required=False,
        default=const.DEFAULTS["id"],
        type=str,
        help="Field to use as an ID; defaults to the input's own internal ID if it has one (e.g. a GPKG's FID column, or a DB table's single-column primary key), otherwise falls back to a constructed 0...n index on the original feature order.",
        nargs=1,
    )
    @click.option(
        "-k",
        "--keep_attributes",
        is_flag=True,
        show_default=True,
        default=const.DEFAULTS["k"],
        help=f"Retain attributes in output. The default is to create an output that only includes {display_name} cell ID and the ID given by the -id field (or the default index ID).",
    )
    @click.option(
        "-ka",
        "--keep_attribute",
        multiple=True,
        default=(),
        type=str,
        help="Retain only this attribute in output; repeat for multiple. Takes precedence over -k/--keep_attributes.",
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
        required=False,
        default=const.DEFAULTS["c"],
        type=float,
        help="Cutting up large geometries into smaller geometries based on a target area. Units are assumed to match the input CRS units unless `--cut_crs` is also given, in which case units match the units of the supplied CRS. If left unspecified, the threshold defaults to the area of a few thousand cells of the target resolution (a benchmarked balance of parallelism against per-piece overhead), converted into the squared units of the cutting CRS. A threshold of 0 will skip bisection entirely (effectively ignoring --cut_crs).",
        nargs=1,
    )
    @click.option(
        "-t",
        "--threads",
        required=False,
        default=const.DEFAULTS["t"],
        show_default="CPU count - 1, capped by available memory",
        type=click.IntRange(min=1),
        help="Amount of threads used for operation",
        nargs=1,
    )
    @click.option(
        "-cp",
        "--compression",
        required=False,
        default=const.DEFAULTS["cp"],
        type=str,
        callback=common.validate_compression,
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
        "--geo",
        required=False,
        default=const.DEFAULTS["geo"],
        type=click.Choice(const.GEOM_TYPES),
        help="Select geometry encoding for the output: 'none' for regular Parquet (no GeoParquet metadata), or 'point'/'polygon' to write GeoParquet (v1.1.0) with the corresponding geometry type.",
        nargs=1,
    )
    @click.option(
        "--tempdir",
        default=const.DEFAULTS["tempdir"],
        show_default="system temp dir",
        type=click.Path(),
        help="Temporary data is created during the execution of this program. This parameter allows you to control where this data will be written.",
    )
    @click.option(
        "-co",
        "--compact",
        is_flag=True,
        help=f"Compact the {display_name} cells up to the parent resolution, grouping by id_field (explicit, auto-detected, or the default 0...n sequence).",
    )
    @click.option("-o", "--overwrite", is_flag=True)
    @click.version_option(version=__version__)
    def command(
        vector_input: str | Path,
        output_directory: str | Path,
        resolution: str,
        parent_res: str,
        id_field: str | None,
        keep_attributes: bool,
        keep_attribute: tuple[str, ...],
        cut_crs: int,
        cut_threshold: float,
        threads: int,
        compression: str,
        layer: str,
        geom_col: str,
        geo: str,
        tempdir: str | Path,
        compact: bool,
        overwrite: bool,
    ):
        tempfile.tempdir = str(tempdir) if tempdir is not None else tempfile.tempdir
        common.raise_rlimit_nofile()

        common.check_resolutions(resolution, parent_res)

        geo = const.GeoOutputMode(geo).value

        con, vector_input = common.db_conn_and_input_path(vector_input)
        output_directory = common.resolve_output_path(output_directory, overwrite)
        common.check_requested_attributes(keep_attribute, vector_input, layer, con)
        common.check_id_field(id_field, vector_input, layer, con)

        cut_crs_obj: pyproj.CRS | None = None
        if cut_crs is not None:
            cut_crs_obj = pyproj.CRS.from_user_input(cut_crs)

        common.index(
            dggs_key,
            vector_input,
            output_directory,
            int(resolution),
            parent_res,
            keep_attributes,
            cut_threshold,
            threads,
            compression=compression,
            cut_crs=cut_crs_obj,
            id_field=id_field,
            con=con,
            layer=layer,
            geom_col=geom_col,
            geo=geo,
            overwrite=overwrite,
            compact=compact,
            keep_attribute=keep_attribute,
        )

    command.help = (
        f"Ingest a vector dataset and index it to the {display_name} DGGS.\n\n"
        "VECTOR_INPUT is the path to input vector geospatial data.\n"
        "OUTPUT_DIRECTORY should be a directory, not a file or database table, "
        "as it will instead be the write location for an Apache Parquet data store."
    )

    return command
