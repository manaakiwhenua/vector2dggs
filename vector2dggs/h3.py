import click
import click_log
import errno
import logging
import os
import multiprocessing
from multiprocessing.dummy import Pool
from pathlib import Path, PurePath
import shutil
import sys
import tempfile
from typing import Union
from urllib.parse import urlparse
import warnings

os.environ["USE_PYGEOS"] = "0"

import dask.dataframe as dd
import dask_geopandas as dgpd
import geopandas as gpd
import h3pandas
import pandas as pd
import pyproj
from shapely.geometry import GeometryCollection
import sqlalchemy
from tqdm import tqdm
from tqdm.dask import TqdmCallback

from . import katana
from vector2dggs import __version__

LOGGER = logging.getLogger(__name__)
click_log.basic_config(LOGGER)
MIN_H3, MAX_H3 = 0, 15

warnings.filterwarnings(
    "ignore"
)  # This is to filter out the polyfill warnings when rows failed to get indexed at a resolution, can be commented out to find missing rows


DEFAULT_PARENT_OFFSET = 6


class ParentResolutionException(Exception):
    pass

class EmptyDataException(Exception):
    pass

def _get_parent_res(parent_res: Union[None, int], resolution: int):
    """
    Uses a parent resolution,
    OR,
    Given a target resolution, returns our recommended parent resolution.

    Used for intermediate re-partioning.
    """
    return (
        int(parent_res)
        if parent_res is not None
        else max(MIN_H3, (resolution - DEFAULT_PARENT_OFFSET))
    )


def polyfill(
    pq_in: Path,
    spatial_sort_col: str,
    resolution: int,
    parent_res: Union[None, int],
    output_directory: str,
) -> None:
    """
    Reads a geoparquet, performs H3 polyfilling,
    and writes out to parquet.
    """
    df = (
        gpd.read_parquet(pq_in)
        .reset_index()
        .drop(columns=[spatial_sort_col])
        .h3.polyfill_resample(resolution, return_geometry=False)
    )
    df = pd.DataFrame(df).drop(columns=["index", "geometry"])
    df.index.rename(f"h3_{resolution:02}", inplace=True)
    parent_res: int = _get_parent_res(parent_res, resolution)
    # Secondary (parent) H3 index, used later for partitioning
    df = df.h3.h3_to_parent(parent_res).reset_index()
    df.to_parquet(
        PurePath(output_directory, pq_in.name),
        engine="auto",
        compression="ZSTD",
    )
    return None


def polyfill_star(args) -> None:
    return polyfill(*args)


def _parent_partitioning(
    input_dir: Path,
    output_dir: Path,
    resolution,
    parent_res: Union[None, int],
    **kwargs,
) -> Path:
    parent_res: int = _get_parent_res(parent_res, resolution)
    with TqdmCallback(desc="Reading spatial partitions"):
        # Set index as parent cell
        ddf = dd.read_parquet(input_dir, engine="pyarrow").set_index(
            f"h3_{parent_res:02}"
        )
    with TqdmCallback(desc="Counting parents"):
        # Count parents, to get target number of partitions
        uniqueh3 = sorted(list(ddf.index.unique().compute()))

    if not len(uniqueh3):
        raise EmptyDataException('No data to write; input data appears empty. Requested output "{output_dir}" will not be written.'.format(output_dir=output_dir))

    LOGGER.debug(
        "Repartitioning into %d partitions, based on parent cells",
        len(uniqueh3) + 1,
    )

    with TqdmCallback(desc="Repartitioning"):
        ddf = (
            ddf.repartition(  # See "notes" on why divisions expects repetition of the last item https://docs.dask.org/en/stable/generated/dask.dataframe.DataFrame.repartition.html
                divisions=(uniqueh3 + [uniqueh3[-1]]), force=True
            )
            .reset_index()
            .set_index(f"h3_{resolution:02}")
            .drop(columns=[f"h3_{parent_res:02}"])
            .to_parquet(
                output_dir,
                overwrite=kwargs.get("overwrite", False),
                engine=kwargs.get("engine", "pyarrow"),
                write_index=True,
                # append=False,
                name_function=lambda i: f"{uniqueh3[i]}.parquet",
                compression=kwargs.get("compression", "ZSTD"),
            )
        )
    LOGGER.debug("Parent cell repartitioning complete")
    return output_dir


def _index(
    input_file: Union[Path, str],
    output_directory: Union[Path, str],
    resolution: int,
    parent_res: Union[None, int],
    keep_attributes: bool,
    npartitions: int,
    spatial_sorting: str,
    cut_threshold: int,
    processes: int,
    id_field: str = None,
    cut_crs: pyproj.CRS = None,
    con: Union[sqlalchemy.engine.Connection, sqlalchemy.engine.Engine] = None,
    table: str = None,
    geom_col: str = "geom",
    overwrite: bool = False,
) -> Path:
    """
    Performs multi-threaded H3 polyfilling on (multi)polygons.
    """

    if table and con:
        # Database connection
        if keep_attributes:
            q = sqlalchemy.text(f"SELECT * FROM {table}")
        elif id_field and not keep_attributes:
            q = sqlalchemy.text(f"SELECT {id_field}, {geom_col} FROM {table}")
        else:
            q = sqlalchemy.text(f"SELECT {geom_col} FROM {table}")
        df = gpd.read_postgis(q, con.connect(), geom_col=geom_col).rename_geometry(
            "geometry"
        )
    else:
        # Read file
        df = gpd.read_file(input_file)

    if cut_crs:
        df = df.to_crs(cut_crs)
    LOGGER.info("Cutting with CRS: %s", df.crs)

    if id_field:
        df = df.set_index(id_field)
    else:
        df = df.reset_index()
        df = df.rename(columns={"index": "fid"}).set_index("fid")

    if not keep_attributes:
        # Remove all attributes except the geometry
        df = df.loc[:, ["geometry"]]

    LOGGER.info("Watch out for ninjas! (Cutting polygons)")
    with tqdm(total=df.shape[0]) as pbar:
        for index, row in df.iterrows():
            df.loc[index, "geometry"] = GeometryCollection(
                katana.katana(row.geometry, cut_threshold)
            )
            pbar.update(1)

    LOGGER.info("Preparing for spatial partitioning....")
    df = (
        df.to_crs(4326)
        .explode(index_parts=False)  # Explode from GeometryCollection
        .explode(index_parts=False)  # Explode multipolygons to polygons
        .reset_index()
    )

    ddf = dgpd.from_geopandas(df, npartitions=npartitions)

    LOGGER.info("Spatially sorting and partitioning (%s)", spatial_sorting)
    ddf = ddf.spatial_shuffle(by=spatial_sorting)
    spatial_sort_col = (
        spatial_sorting
        if spatial_sorting == "geohash"
        else f"{spatial_sorting}_distance"
    )

    with tempfile.TemporaryDirectory(suffix=".parquet") as tmpdir:
        with TqdmCallback():
            ddf.to_parquet(tmpdir, overwrite=True)

        filepaths = list(map(lambda f: f.absolute(), Path(tmpdir).glob("*")))

        # Multithreaded polyfilling
        LOGGER.info(
            "H3 Indexing on spatial partitions by polyfill with H3 resolution: %d",
            resolution,
        )
        with tempfile.TemporaryDirectory(suffix=".parquet") as tmpdir2:
            with Pool(processes=processes) as pool:
                args = [
                    (filepath, spatial_sort_col, resolution, parent_res, tmpdir2)
                    for filepath in filepaths
                ]
                list(tqdm(pool.imap(polyfill_star, args), total=len(args)))

            output_directory = _parent_partitioning(
                tmpdir2, output_directory, resolution, parent_res, overwrite=overwrite
            )

    return output_directory


@click.command(context_settings={"show_default": True})
@click_log.simple_verbosity_option(LOGGER)
@click.argument("vector_input", required=True, type=click.Path(), nargs=1)
@click.argument("output_directory", required=True, type=click.Path(), nargs=1)
@click.option(
    "-r",
    "--resolution",
    required=True,
    type=click.Choice(list(map(str, range(MIN_H3, MAX_H3 + 1)))),
    help="H3 resolution to index",
    nargs=1,
)
@click.option(
    "-pr",
    "--parent_res",
    required=False,
    type=click.Choice(list(map(str, range(MIN_H3, MAX_H3 + 1)))),
    help="H3 Parent resolution for the output partition. Defaults to resolution - 6",
)
@click.option(
    "-id",
    "--id_field",
    required=False,
    default=None,
    type=str,
    help="Field to use as an ID; defaults to a constructed single 0...n index on the original feature order.",
    nargs=1,
)
@click.option(
    "-k",
    "--keep_attributes",
    is_flag=True,
    show_default=True,
    default=False,
    help="Retain attributes in output. The default is to create an output that only includes H3 cell ID and the ID given by the -id field (or the default index ID).",
)
@click.option(
    "-p",
    "--partitions",
    required=True,
    type=int,
    default=50,
    help="The number of partitions to create. Recommendation: at least as many partitions as there are available `--threads`. Partitions are processed in parallel once they have been formed.",
    nargs=1,
)
@click.option(
    "-s",
    "--spatial_sorting",
    type=click.Choice(["hilbert", "morton", "geohash"]),
    default="hilbert",
    help="Spatial sorting method when perfoming spatial partitioning.",
)
@click.option(
    "-crs",
    "--cut_crs",
    required=False,
    default=None,
    type=int,
    help="Set the coordinate reference system (CRS) used for cutting large polygons (see `--cur-threshold`). Defaults to the same CRS as the input. Should be a valid EPSG code.",
    nargs=1,
)
@click.option(
    "-c",
    "--cut_threshold",
    required=True,
    default=5000,
    type=int,
    help="Cutting up large polygons into smaller pieces based on a target length. Units are assumed to match the input CRS units unless the `--cut_crs` is also given, in which case units match the units of the supplied CRS.",
    nargs=1,
)
@click.option(
    "-t",
    "--threads",
    required=False,
    default=7,
    type=int,
    help="Amount of threads used for operation",
    nargs=1,
)
@click.option(
    "-tbl",
    "--table",
    required=False,
    default=None,
    type=str,
    help="Name of the table to read when using a spatial database connection as input",
    nargs=1,
)
@click.option(
    "-g",
    "--geom_col",
    required=False,
    default="geom",
    type=str,
    help="Column name to use when using a spatial database connection as input",
    nargs=1,
)
@click.option("-o", "--overwrite", is_flag=True)
@click.version_option(version=__version__)
def h3(
    vector_input: Union[str, Path],
    output_directory: Union[str, Path],
    resolution: str,
    parent_res: str,
    id_field: str,
    keep_attributes: bool,
    partitions: int,
    spatial_sorting: str,
    cut_crs: int,
    cut_threshold: int,
    threads: int,
    table: str,
    geom_col: str,
    overwrite: bool,
):
    """
    Ingest a vector dataset and index it to the H3 DGGS.

    VECTOR_INPUT is the path to input vector geospatial data.
    OUTPUT_DIRECTORY should be a directory, not a file or database table, as it will instead be the write location for an Apache Parquet data store.
    """
    if parent_res is not None and not int(parent_res) < int(resolution):
        raise ParentResolutionException(
            "Parent resolution ({pr}) must be less than target resolution ({r})".format(
                pr=parent_res, r=resolution
            )
        )
    con: sqlalchemy.engine.Connection = None
    scheme: str = urlparse(vector_input).scheme
    if bool(scheme) and scheme != "file":
        # Assume database connection
        con = sqlalchemy.create_engine(vector_input)
    elif not Path(vector_input).exists():
        if not scheme:
            LOGGER.warning(
                f"Input vector {vector_input} does not exist, and is not recognised as a remote URI"
            )
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), vector_input
            )
        vector_input = str(vector_input)
    else:
        vector_input = Path(vector_input)

    output_directory = Path(output_directory)
    outputexists = os.path.exists(output_directory)
    if outputexists and not overwrite:
        raise FileExistsError(
            f"{output_directory} already exists; if you want to overwrite this, use the -o/--overwrite flag"
        )
    elif outputexists and overwrite:
        LOGGER.info(f"Overwriting the contents of {output_directory}")
        shutil.rmtree(output_directory)
    output_directory.mkdir(parents=True, exist_ok=True)

    if cut_crs is not None:
        cut_crs = pyproj.CRS.from_user_input(cut_crs)

    try:
        _index(
            vector_input,
            output_directory,
            int(resolution),
            parent_res,
            keep_attributes,
            partitions,
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
