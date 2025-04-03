import os
import errno
import logging
import tempfile
import click_log
import sqlalchemy
import shutil
import pyproj

import pandas as pd
import geopandas as gpd
import dask.dataframe as dd
import dask_geopandas as dgpd

from typing import Union, Callable
from pathlib import Path, PurePath
from urllib.parse import urlparse
from tqdm import tqdm
from tqdm.dask import TqdmCallback
from multiprocessing.dummy import Pool
from shapely.geometry import GeometryCollection

import vector2dggs.constants as const

from . import katana

SQLConnectionType = Union[sqlalchemy.engine.Connection, sqlalchemy.engine.Engine]


LOGGER = logging.getLogger(__name__)
click_log.basic_config(LOGGER)
click_log.ColorFormatter.colors["info"] = dict(fg="green")


class ParentResolutionException(Exception):
    pass


def check_resolutions(resolution: int, parent_res: int) -> None:
    if parent_res is not None and not int(parent_res) < int(resolution):
        raise ParentResolutionException(
            "Parent resolution ({pr}) must be less than target resolution ({r})".format(
                pr=parent_res, r=resolution
            )
        )


def db_conn_and_input_path(
    vector_input: Union[str, Path],
) -> tuple[SQLConnectionType, Union[str, Path]]:
    con: sqlalchemy.engine.Connection = None
    scheme: str = urlparse(vector_input).scheme

    if bool(scheme) and scheme != "file":
        # Assume database connection
        con = sqlalchemy.create_engine(vector_input)

    elif not Path(vector_input).exists():
        if not scheme:
            LOGGER.error(
                f"Input vector {vector_input} does not exist, and is not recognised as a remote URI"
            )
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), vector_input
            )
        vector_input = str(vector_input)

    else:
        vector_input = Path(vector_input)

    return (con, vector_input)


def resolve_output_path(
    output_directory: Union[str, Path], overwrite: bool
) -> Union[str, Path]:
    output_directory = Path(output_directory)
    outputexists = os.path.exists(output_directory)

    if outputexists and not overwrite:
        raise FileExistsError(
            f"{output_directory} already exists; if you want to overwrite this, use the -o/--overwrite flag"
        )

    elif outputexists and overwrite:
        LOGGER.warning(f"Overwriting the contents of {output_directory}")
        shutil.rmtree(output_directory)

    output_directory.mkdir(parents=True, exist_ok=True)

    return output_directory


def drop_condition(
    df: pd.DataFrame,
    drop_index: pd.Index,
    log_statement: str,
    warning_threshold: float = 0.01,
):
    LOGGER.debug(log_statement)
    _before = len(df)
    df = df.drop(drop_index)
    _after = len(df)
    _diff = _before - _after
    if _diff:
        log_method = (
            LOGGER.info if (_diff / float(_before)) < warning_threshold else LOGGER.warn
        )
        log_method(f"Dropped {_diff} rows ({_diff/float(_before)*100:.2f}%)")
    return df


def get_parent_res(dggs: str, parent_res: Union[None, int], resolution: int):
    """
    Uses a parent resolution,
    OR,
    Given a target resolution, returns our recommended parent resolution.

    Used for intermediate re-partioning.
    """
    if dggs == "h3":
        return (
            parent_res
            if parent_res is not None
            else max(const.MIN_H3, (resolution - const.DEFAULT_PARENT_OFFSET))
        )
    elif dggs == "rhp":
        return (
            parent_res
            if parent_res is not None
            else max(const.MIN_RHP, (resolution - const.DEFAULT_PARENT_OFFSET))
        )
    else:
        raise RuntimeError(
            "Unknown dggs {dggs}) -  must be one of [ 'h3', 'rhp' ]".format(dggs=dggs)
        )


def parent_partitioning(
    dggs: str,
    input_dir: Path,
    output_dir: Path,
    resolution: int,
    parent_res: Union[None, int],
    **kwargs,
) -> None:
    parent_res: int = get_parent_res(dggs, parent_res, resolution)
    partition_col = f"{dggs}_{parent_res:02}"

    with TqdmCallback(desc="Repartitioning"):
        dd.read_parquet(input_dir, engine="pyarrow").to_parquet(
            output_dir,
            overwrite=kwargs.get("overwrite", False),
            engine=kwargs.get("engine", "pyarrow"),
            partition_on=partition_col,
            compression=kwargs.get("compression", "ZSTD"),
        )
    LOGGER.debug("Parent cell repartitioning complete")

    # Rename output to just be the partition key, suffix .parquet
    for f in os.listdir(output_dir):
        os.rename(
            os.path.join(output_dir, f),
            os.path.join(output_dir, f.replace(f"{partition_col}=", "") + ".parquet"),
        )

    return


def polyfill(
    dggs: str,
    dggsfunc: Callable,
    secondary_index_func: Callable,
    pq_in: Path,
    spatial_sort_col: str,
    resolution: int,
    parent_res: Union[None, int],
    output_directory: str,
) -> None:
    """
    Reads a geoparquet, performs polyfilling (for Polygon),
    linetracing (for LineString), and writes out to parquet.
    """
    df = gpd.read_parquet(pq_in).reset_index().drop(columns=[spatial_sort_col])
    if len(df.index) == 0:
        # Input is empty, nothing to polyfill
        return None

    # DGGS specific polyfill
    df = dggsfunc(df, resolution)

    if len(df.index) == 0:
        # Polyfill resulted in empty output (e.g. large cell, small feature)
        return None

    df.index.rename(f"{dggs}_{resolution:02}", inplace=True)
    parent_res: int = get_parent_res(dggs, parent_res, resolution)
    # print(parent_res)
    # print(df.index)
    # print(df.columns)

    # Secondary (parent) index, used later for partitioning
    df = secondary_index_func(df, parent_res)

    df.to_parquet(
        PurePath(output_directory, pq_in.name), engine="auto", compression="ZSTD"
    )
    return None


def polyfill_star(args) -> None:
    return polyfill(*args)


def index(
    dggs: str,
    dggsfunc: Callable,
    secondary_index_func: Callable,
    input_file: Union[Path, str],
    output_directory: Union[Path, str],
    resolution: int,
    parent_res: Union[None, int],
    keep_attributes: bool,
    chunksize: int,
    spatial_sorting: str,
    cut_threshold: int,
    processes: int,
    id_field: str = None,
    cut_crs: pyproj.CRS = None,
    con: SQLConnectionType = None,
    table: str = None,
    geom_col: str = "geom",
    overwrite: bool = False,
) -> Path:
    """
    Performs multi-threaded polyfilling on (multi)polygons.
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
    LOGGER.debug("Cutting with CRS: %s", df.crs)

    if id_field:
        df = df.set_index(id_field)
    else:
        df = df.reset_index()
        df = df.rename(columns={"index": "fid"}).set_index("fid")

    if not keep_attributes:
        # Remove all attributes except the geometry
        df = df.loc[:, ["geometry"]]

    LOGGER.debug("Cutting large geometries")
    with tqdm(total=df.shape[0], desc="Splitting") as pbar:
        for index, row in df.iterrows():
            df.loc[index, "geometry"] = GeometryCollection(
                katana.katana(row.geometry, cut_threshold)
            )
            pbar.update(1)

    LOGGER.debug("Exploding geometry collections and multipolygons")
    df = (
        df.to_crs(4326)
        .explode(index_parts=False)  # Explode from GeometryCollection
        .explode(index_parts=False)  # Explode multipolygons to polygons
    ).reset_index()

    drop_conditions = [
        {
            "index": lambda frame: frame[
                (frame.geometry.is_empty | frame.geometry.isna())
            ],
            "message": "Considering empty or null geometries",
        },
        {
            "index": lambda frame: frame[
                (frame.geometry.geom_type != "Polygon")
                & (frame.geometry.geom_type != "LineString")
            ],  # NB currently points and other types are lost; in principle, these could be indexed
            "message": "Considering unsupported geometries",
        },
    ]
    for condition in drop_conditions:
        df = drop_condition(df, condition["index"](df).index, condition["message"])

    ddf = dgpd.from_geopandas(df, chunksize=max(1, chunksize), sort=True)

    LOGGER.debug("Spatially sorting and partitioning (%s)", spatial_sorting)
    ddf = ddf.spatial_shuffle(by=spatial_sorting)
    spatial_sort_col = (
        spatial_sorting
        if spatial_sorting == "geohash"
        else f"{spatial_sorting}_distance"
    )

    with tempfile.TemporaryDirectory(suffix=".parquet") as tmpdir:
        with TqdmCallback(desc=f"Spatially partitioning"):
            ddf.to_parquet(tmpdir, overwrite=True)

        filepaths = list(map(lambda f: f.absolute(), Path(tmpdir).glob("*")))

        # Multithreaded polyfilling
        LOGGER.debug(
            "Indexing on spatial partitions by polyfill with resolution: %d",
            resolution,
        )
        with tempfile.TemporaryDirectory(suffix=".parquet") as tmpdir2:
            with Pool(processes=processes) as pool:
                args = [
                    (
                        dggs,
                        dggsfunc,
                        secondary_index_func,
                        filepath,
                        spatial_sort_col,
                        resolution,
                        parent_res,
                        tmpdir2,
                    )
                    for filepath in filepaths
                ]
                list(
                    tqdm(
                        pool.imap(polyfill_star, args),
                        total=len(args),
                        desc="DGGS indexing",
                    )
                )

            parent_partitioning(
                dggs,
                tmpdir2,
                output_directory,
                resolution,
                parent_res,
                overwrite=overwrite,
            )

    return output_directory
