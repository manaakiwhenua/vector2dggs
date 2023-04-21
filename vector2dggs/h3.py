import click
import click_log
import errno
import logging
import os
import multiprocessing
from multiprocessing.dummy import Pool
from pathlib import Path, PurePath
import shutil
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
from shapely.geometry import GeometryCollection
from tqdm import tqdm
from tqdm.dask import TqdmCallback

from . import katana

LOGGER = logging.getLogger(__name__)
click_log.basic_config(LOGGER)
MIN_H3, MAX_H3 = 0, 15

warnings.filterwarnings(
    "ignore"
)  # This is to filter out the polyfill warnings when rows failed to get indexed at a resolution, can be commented out to find missing rows


def _index(
    input_file: Union[Path, str],
    output_directory: Union[Path, str],
    resolution: int,
    id_field: str,
    all_attributes: bool,
    npartitions: int,
    spatial_sorting: str,
    cut_crs: int,
    cut_threshold: int,
    processes: int,
) -> Path:
    """
    Performs multi-threaded H3 polyfilling on (multi)polygons.
    """

    df = gpd.read_file(input_file)

    if cut_crs:
        df=df.to_crs(cut_crs) 
    LOGGER.info("Cutting with crs:"+ str(df.crs))

    if id_field:
        df = df.set_index(id_field)
    else:
        df=df.reset_index()
        df=df.rename(columns={'index':'fid'}).set_index('fid')

    if not all_attributes:
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

    with tempfile.TemporaryDirectory() as tmpdir:
        with TqdmCallback():
            ddf.to_parquet(tmpdir, overwrite=True)

        filepaths = list(map(lambda f: f.absolute(), Path(tmpdir).glob("*")))

        # Polyfilling function defined here
        def polyfill(pq_in: Path) -> None:
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
            df.to_parquet(
                PurePath(output_directory, pq_in.name),
                engine="auto",
                compression="ZSTD",  #  TODO parameterise
            )
            return None

        # Multithreaded polyfilling
        LOGGER.info(
            "H3 Indexing on spatial partitions by polyfill with H3 resoltion: %d",
            resolution,
        )
        with Pool(processes=processes) as pool:
            list(tqdm(pool.imap(polyfill, filepaths), total=len(filepaths)))


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
    "-id",
    "--id_field",
    required=False,
    default=None,
    type=str,
    help="Field to use as an ID; defaults to a constructed single 0...n index on the original feature order.",
    nargs=1,
)
@click.option(
    "-a",
    "--all_attributes",
    is_flag=True,
    show_default=True,
    default=False,
    help="Retain attributes in output. The default is to create an output that only includes H3 cell ID and the ID(s) given by the -id field (or the default index ID).",
)
@click.option(
    "-p",
    "--partitions",
    required=True,
    type=int,
    default=50,
    help="Geo-partitioning, currently only available in Hilbert method",
    nargs=1,
)
@click.option(
    "-s",
    "--spatial-sorting",
    type=click.Choice(["hilbert", "morton", "geohash"]),
    default="hilbert",
    help="Spatial sorting method",
)
@click.option(
    "-crs",
    "--cut_crs",
    required=False,
    default=None,
    type=int,
    help="Set crs(epsg) to input layer (used for cutting), defaults to input crs",
    nargs=1,
)
@click.option(
    "-c",
    "--cut_threshold",
    required=True,
    default=5000,
    type=int,
    help="Cutting up large polygons into target length (in set crs units)",
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
@click.option("-o", "--overwrite", is_flag=True)
def h3(
    vector_input: Union[str, Path],
    output_directory: Union[str, Path],
    resolution: str,
    id_field: str,
    all_attributes: bool,
    partitions: int,
    spatial_sorting: str,
    cut_crs:int,
    cut_threshold: int,
    threads: int,
    overwrite: bool,
):
    """
    Ingest a vector dataset and index it to the H3 DGGS.

    VECTOR_INPUT is the path to input vector geospatial data.
    OUTPUT_DIRECTORY should be a directorty, not a file, as it will be the write location for an Apache Parquet data store.
    """
    if not Path(vector_input).exists():
        if not urlparse(vector_input).scheme:
            LOGGER.warning(
                f"Input vector {vector_input} does not exist, and is not recognised as a remote URI"
            )
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), input_file)
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

    _index(
        vector_input,
        output_directory,
        int(resolution),
        id_field,
        all_attributes,
        partitions,
        spatial_sorting,
        cut_crs,
        cut_threshold,
        threads,
    )
