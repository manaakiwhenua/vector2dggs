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

os.environ['USE_PYGEOS'] = '0'

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
    cut_threshold: int,
    processes: int
) -> Path:
    '''
    TODO write a docstring
    '''

    df = gpd.read_file(input_file).to_crs(2193) # Reproj to equal area projection
    if id_field:
        df = df.set_index(id_field)

    if not all_attributes:
        # Remove all attributes except the geometry
        df = df.loc[:, ["geometry"]]

    LOGGER.info("Watch out for ninjas! (Cutting polygons)")
    with tqdm(total=df.shape[0]) as pbar:
        for index, row in df.iterrows():
            df.loc[index, "geometry"] = GeometryCollection(katana.katana(row.geometry, cut_threshold))
            pbar.update(1)

    LOGGER.info("Preparing for spatial partitioning")
    df = df.to_crs(
        4326
    ).explode( # Explode from GeometryCollection
        index_parts=False
    ).explode( # Explode multipolygons to polygons
        index_parts=False
    ).reset_index()

    ddf = dgpd.from_geopandas(df, npartitions=npartitions)

    spatial_partioning_method = 'hilbert' # TODO paramerterise enum of spatial partitioning methods
    LOGGER.info("Spatial partitioning (%s) with %d partitions", spatial_partioning_method, npartitions)
    ddf = ddf.spatial_shuffle(by=spatial_partioning_method)

    with tempfile.TemporaryDirectory() as tmpdir:
        with TqdmCallback():
            ddf.to_parquet(tmpdir, overwrite=True)

        filepaths = map(lambda f: f.absolute(), Path(tmpdir).glob('*'))

        # Polyfilling function defined here
        def polyfill(pq_in: Path) -> None:
            """
            Reads a geoparquet, performs H3 polyfilling,
            and writes out to parquet.
            """
            df = gpd.read_parquet(pq_in).reset_index().drop(
                columns=["hilbert_distance"]
            ).h3.polyfill_resample(
                resolution, return_geometry=False
            )
            pd.DataFrame(df).drop(
                columns=["index", "geometry"]
            ).to_parquet(
                PurePath(output_directory, pq_in.name),
                engine='auto',
                compression='ZSTD' #  TODO parameterise
            )
            return None

        # Multithreaded polyfilling
        LOGGER.info(
            "H3 Indexing on spatial partitions by polyfill with H3 resoltion: %d",
            resolution,
        )
        # TODO progress bar
        with Pool(processes=processes) as pool:
            # have your pool map the file names to dataframes
            # pool.map(polyfill, filepaths)
            list(tqdm(pool.imap(polyfill, filepaths), total=npartitions))

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
    "-c",
    "--cut_threshold",
    required=True,
    default=5000,
    type=int,
    help="Cutting up large polygons into target length (meters)",
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
    id_field : str,
    all_attributes: bool,
    partitions: int,
    cut_threshold: int,
    threads: int,
    overwrite: bool
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
        raise FileExistsError(f'{output_directory} already exists; if you want to overwrite this, use the -o/--overwrite flag')
    elif outputexists and overwrite:
        LOGGER.info(f'Overwriting the contents of {output_directory}')
        shutil.rmtree(output_directory)
    output_directory.mkdir(parents=True, exist_ok=True)

    _index(
        vector_input,
        output_directory,
        int(resolution),
        id_field,
        all_attributes,
        partitions,
        cut_threshold,
        threads
    )