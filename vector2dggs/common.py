import os
import errno
import logging
import tempfile
import click_log
import sqlalchemy
import shutil
import pyproj
from uuid import uuid4

import pandas as pd
import geopandas as gpd
import dask.dataframe as dd
import dask_geopandas as dgpd

from typing import Union, Callable, Iterable
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


class IdFieldError(ValueError):
    """Raised when an invalid or missing ID field is provided."""

    pass


def check_resolutions(resolution: int, parent_res: int) -> None:
    if parent_res is not None and not int(parent_res) < int(resolution):
        raise ParentResolutionException(
            "Parent resolution ({pr}) must be less than target resolution ({r})".format(
                pr=parent_res, r=resolution
            )
        )


def check_compaction_requirements(compact: bool, id_field: Union[str, None]) -> None:
    if compact and not id_field:
        raise IdFieldError(
            "An id_field is required for compaction, in order to handle the potential for overlapping features"
        )


def compaction(
    df: pd.DataFrame,
    res: int,
    id_field: str,
    col_order: list[str],
    dggs_col: str,
    compact_func: Callable[[Iterable[Union[str, int]]], Iterable[Union[str, int]]],
    cell_to_child_func: Callable[[Union[str, int], int], Union[str, int]],
):
    """
    Compacts a dataframe up to a given low resolution (parent_res), from an existing maximum resolution (res).
    """
    df = df.reset_index(drop=False)

    feature_cell_groups = (
        df.groupby(id_field)[dggs_col].apply(lambda x: set(x)).to_dict()
    )
    feature_cell_compact = {
        id: set(compact_func(cells)) for id, cells in feature_cell_groups.items()
    }

    uncompressable = {
        id: feature_cell_groups[id] & feature_cell_compact[id]
        for id in feature_cell_groups.keys()
    }
    compressable = {
        id: feature_cell_compact[id] - feature_cell_groups[id]
        for id in feature_cell_groups.keys()
    }

    # Get rows that cannot be compressed
    mask = pd.Series([False] * len(df), index=df.index)  # Init bool mask
    for key, value_set in uncompressable.items():
        mask |= (df[id_field] == key) & (df[dggs_col].isin(value_set))
    uncompressable_df = df[mask].set_index(dggs_col)

    # Get rows that can be compressed
    # Convert each compressed (coarser resolution) cell into a cell at
    #   the original resolution (usu using centre child as reference)
    compression_mapping = {
        (id, cell_to_child_func(cell, res)): cell
        for id, cells in compressable.items()
        if cells
        for cell in cells
    }
    mask = pd.Series([False] * len(df), index=df.index)
    composite_key = f"composite_key_{uuid4()}"
    # Update mask for compressible rows and prepare for replacement
    get_composite_key = lambda row: (row[id_field], row[dggs_col])
    df[composite_key] = df.apply(get_composite_key, axis=1)
    mask |= df[composite_key].isin(compression_mapping)
    compressable_df = df[mask].copy()
    compressable_df[dggs_col] = compressable_df[composite_key].map(
        compression_mapping
    )  # Replace DGGS cell ID with compressed representation
    compressable_df = compressable_df.set_index(dggs_col)

    return pd.concat([compressable_df, uncompressable_df])[col_order]


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
            LOGGER.info
            if (_diff / float(_before)) < warning_threshold
            else LOGGER.warning
        )
        log_method(f"Dropped {_diff} rows ({_diff/float(_before)*100:.2f}%)")
    return df


def get_parent_res(dggs: str, parent_res: Union[None, str], resolution: int) -> int:
    """
    Uses a parent resolution,
    OR,
    Given a target resolution, returns our recommended parent resolution.

    Used for intermediate re-partioning.
    """
    if not dggs in const.DEFAULT_DGGS_PARENT_RES.keys():
        raise RuntimeError(
            "Unknown dggs {dggs}) -  must be one of [ {options} ]".format(
                dggs=dggs, options=", ".join(const.DEFAULT_DGGS_PARENT_RES.keys())
            )
        )
    return (
        int(parent_res)
        if parent_res is not None
        else const.DEFAULT_DGGS_PARENT_RES[dggs](resolution)
    )


def parent_partitioning(
    dggs: str,
    input_dir: Path,
    output_dir: Path,
    compaction_func: Union[Callable, None],
    resolution: int,
    parent_res: int,
    id_field: str,
    **kwargs,
) -> None:
    partition_col = f"{dggs}_{parent_res:02}"
    dggs_col = f"{dggs}_{resolution:02}"

    # Read the parquet files into a Dask DataFrame
    ddf = dd.read_parquet(input_dir, engine="pyarrow")
    meta = ddf._meta

    with TqdmCallback(
        desc=f"Parent partitioning, writing {'compacted ' if compaction_func else ''}output"
    ):
        if compaction_func:
            # Apply the compaction function to each partition
            unique_parents = sorted(
                [v for v in ddf[partition_col].unique().compute() if pd.notna(v)]
            )
            divisions = unique_parents + [unique_parents[-1]]
            ddf = (
                ddf.reset_index(drop=False)
                .dropna(subset=[partition_col])
                .set_index(partition_col)
                .repartition(divisions=divisions)
                .map_partitions(
                    compaction_func,
                    resolution,
                    meta.columns.to_list(),  # Column order to be returned
                    dggs_col,
                    id_field,
                    meta=meta,
                )
            )

        ddf.to_parquet(
            output_dir,
            overwrite=kwargs.get("overwrite", False),
            engine=kwargs.get("engine", "pyarrow"),
            partition_on=[partition_col],
            compression=kwargs.get("compression", "ZSTD"),
            # **kwargs
        )

    LOGGER.debug("Parent cell partitioning complete")

    # Append a .parquet suffix
    for f in os.listdir(output_dir):
        os.rename(
            os.path.join(output_dir, f),
            os.path.join(output_dir, f.replace(f"{partition_col}=", "")),
        )

    return


def polyfill(
    dggs: str,
    dggsfunc: Callable,
    secondary_index_func: Callable,
    pq_in: Path,
    spatial_sort_col: str,
    resolution: int,
    parent_res: int,
    output_directory: str,
    compression: str = "snappy",
) -> None:
    """
    Reads a geoparquet, performs polyfilling (for Polygon),
    linetracing (for LineString), or indexing (for Point),
    and writes out to parquet.
    """
    df = gpd.read_parquet(pq_in).reset_index()
    if spatial_sort_col != "none":
        df = df.drop(columns=[spatial_sort_col])
    if len(df.index) == 0:
        # Input is empty, nothing to convert
        return None

    # DGGS specific conversion
    df = dggsfunc(df, resolution)

    if len(df.index) == 0:
        # Conversion resulted in empty output (e.g. large cell, small feature)
        return None

    df.index.rename(f"{dggs}_{resolution:02}", inplace=True)

    # Secondary (parent) index, used later for partitioning
    df = secondary_index_func(df, parent_res)

    df.to_parquet(
        PurePath(output_directory, pq_in.name), engine="auto", compression=compression
    )
    return None


def polyfill_star(args) -> None:
    return polyfill(*args)


def index(
    dggs: str,
    dggsfunc: Callable,
    secondary_index_func: Callable,
    compaction_func: Union[Callable, None],
    input_file: Union[Path, str],
    output_directory: Union[Path, str],
    resolution: int,
    parent_res: Union[None, int],
    keep_attributes: bool,
    chunksize: int,
    spatial_sorting: str,
    cut_threshold: int,
    processes: int,
    compression: str = "snappy",
    id_field: str = None,
    cut_crs: pyproj.CRS = None,
    con: SQLConnectionType = None,
    layer: str = None,
    geom_col: str = "geom",
    overwrite: bool = False,
) -> Path:
    """
    Performs multi-threaded DGGS indexing on geometries (including multipart and collections).
    """
    parent_res = get_parent_res(dggs, parent_res, resolution)

    if layer and con:
        # Database connection
        if keep_attributes:
            q = sqlalchemy.text(f"SELECT * FROM {layer}")
        elif id_field and not keep_attributes:
            q = sqlalchemy.text(f"SELECT {id_field}, {geom_col} FROM {layer}")
        else:
            q = sqlalchemy.text(f"SELECT {geom_col} FROM {layer}")
        df = gpd.read_postgis(q, con.connect(), geom_col=geom_col).rename_geometry(
            "geometry"
        )
    else:
        # Read file
        df = gpd.read_file(input_file, layer=layer)

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
                & (frame.geometry.geom_type != "Point")
            ],
            "message": "Considering unsupported geometries",
        },
    ]
    for condition in drop_conditions:
        df = drop_condition(df, condition["index"](df).index, condition["message"])

    ddf = dgpd.from_geopandas(df, chunksize=max(1, chunksize), sort=True)

    if spatial_sorting != "none":
        LOGGER.debug("Spatially sorting and partitioning (%s)", spatial_sorting)
        ddf = ddf.spatial_shuffle(by=spatial_sorting)
    spatial_sort_col = (
        spatial_sorting
        if (spatial_sorting == "geohash" or spatial_sorting == "none")
        else f"{spatial_sorting}_distance"
    )

    with tempfile.TemporaryDirectory(suffix=".parquet") as tmpdir:
        with TqdmCallback(desc=f"Spatially partitioning"):
            ddf.to_parquet(tmpdir, overwrite=True)

        filepaths = list(map(lambda f: f.absolute(), Path(tmpdir).glob("*")))

        # Multithreaded DGGS indexing
        LOGGER.debug(
            "DGGS indexing by spatial partitions with resolution: %d",
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
                        compression,
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
                Path(tmpdir2),
                output_directory,
                compaction_func,
                resolution,
                parent_res,
                id_field,
                overwrite=overwrite,
                compression=compression,
            )

    return output_directory
