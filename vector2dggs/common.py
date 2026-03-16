import os
import errno
import json
import logging
import tempfile
import click_log
import sqlalchemy
import shutil
import pyproj
from uuid import uuid4

import pandas as pd
import geopandas as gpd
import dask
import dask.dataframe as dd
import dask_geopandas as dgpd
import numpy as np
import shapely
import pyarrow as pa
import pyarrow.parquet as pq

from typing import Union, Iterable  # , Callable
from pathlib import Path, PurePath
from urllib.parse import urlparse
from tqdm import tqdm
from tqdm.dask import TqdmCallback
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from shapely.geometry import GeometryCollection

import vector2dggs.constants as const
import vector2dggs.indexerfactory as idxfactory

from . import katana
from vector2dggs.indexers.vectorindexer import VectorIndexer

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


def write_partition_as_geoparquet(
    partition_df: pd.DataFrame,
    geo_serialisation_method,
    output_dir: Path,
    partition_col: str,
    dggs_col: str,
    compression: str,
) -> int:
    if len(partition_df.index) == 0:
        return 0

    if (
        partition_col not in partition_df.columns
        and partition_df.index.name == partition_col
    ):
        partition_df = partition_df.reset_index(drop=False)

    if partition_col not in partition_df.columns:
        raise KeyError(
            f"Could not find partition column '{partition_col}' in partition write step"
        )

    # Build shapely geometries for this dask partition
    if dggs_col in partition_df.columns:
        geoms = partition_df[dggs_col].map(geo_serialisation_method)
    else:
        geoms = pd.Series(
            partition_df.index.map(geo_serialisation_method), index=partition_df.index
        )

    # Compute optional GeoParquet bbox / geometry_types metadata (vectorized)
    geom_arr = geoms.to_numpy()
    valid_mask = pd.notna(geom_arr)
    if valid_mask.any():
        valid_mask[valid_mask] &= ~shapely.is_empty(geom_arr[valid_mask])

    if valid_mask.any():
        bounds = np.asarray(shapely.bounds(geom_arr[valid_mask]))
        bounds = np.atleast_2d(bounds)
        finite_mask = ~np.isnan(bounds).any(axis=1)
        bbox_vals = bounds[finite_mask]

        if len(bbox_vals):
            bbox = [
                float(np.min(bbox_vals[:, 0])),
                float(np.min(bbox_vals[:, 1])),
                float(np.max(bbox_vals[:, 2])),
                float(np.max(bbox_vals[:, 3])),
            ]
        else:
            bbox = None

        valid_geoms = geom_arr[valid_mask]
        geometry_types = sorted({g.geom_type for g in valid_geoms})
    else:
        bbox = None
        geometry_types = []

    pdf = partition_df.copy()
    pdf[partition_col] = pdf[partition_col].astype("string")
    pdf["geometry"] = shapely.to_wkb(geoms, hex=False)

    table = pa.Table.from_pandas(pdf, preserve_index=True)

    # Ensure geometry field is Binary
    geom_idx = table.schema.get_field_index("geometry")
    if geom_idx >= 0 and not pa.types.is_binary(table.field(geom_idx).type):
        geom_array = pa.array(table.column(geom_idx).to_pylist(), type=pa.binary())
        table = table.set_column(geom_idx, "geometry", geom_array)

    col_meta = {
        "encoding": "WKB",
        "crs": pyproj.CRS.from_epsg(4326).to_json_dict(),
    }
    if geometry_types:
        col_meta["geometry_types"] = geometry_types
    if bbox is not None:
        col_meta["bbox"] = bbox

    geo_meta = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {"geometry": col_meta},
    }
    existing_meta = table.schema.metadata or {}
    new_meta = {**existing_meta, b"geo": json.dumps(geo_meta).encode("utf-8")}
    table = table.replace_schema_metadata(new_meta)

    pq.write_to_dataset(
        table,
        root_path=str(output_dir),
        partition_cols=[partition_col],
        compression=compression,
        basename_template=f"part.{{i}}-{uuid4().hex}.parquet",
        use_threads=True,
    )

    return int(len(pdf.index) > 0)


def parent_partitioning(
    indexer: VectorIndexer,
    input_dir: Path,
    output_dir: Path,
    resolution: int,
    parent_res: int,
    id_field: str,
    compact: bool,
    geo: str,
    **kwargs,
) -> None:
    partition_col = f"{indexer.dggs}_{parent_res:02}"
    dggs_col = f"{indexer.dggs}_{resolution:02}"

    # Read the parquet files into a Dask DataFrame
    ddf = dd.read_parquet(input_dir, engine="pyarrow")
    meta = ddf._meta

    with TqdmCallback(
        desc=f"Parent partitioning, writing {'compacted ' if compact else ''}output"
    ):

        if compact:
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
                    indexer.compaction,
                    resolution,
                    meta.columns.to_list(),  # Column order to be returned
                    dggs_col,
                    id_field,
                    meta=meta,
                )
            )

        if geo == const.GeoOutputMode.NONE.value:
            ddf.to_parquet(
                output_dir,
                overwrite=kwargs.get("overwrite", False),
                engine=kwargs.get("engine", "pyarrow"),
                partition_on=[partition_col],
                write_index=True,
                append=False,
                compression=kwargs.get("compression", "ZSTD"),
                # **kwargs
            )
        else:
            if geo not in (
                const.GeoOutputMode.POINT.value,
                const.GeoOutputMode.POLYGON.value,
            ):
                raise ValueError(
                    f"Unknown geo output mode '{geo}'. Expected one of {const.GEOM_TYPES}."
                )

            geom_fn = (
                indexer.cell_to_point
                if geo == const.GeoOutputMode.POINT.value
                else indexer.cell_to_polygon
            )

            delayed_parts = ddf.to_delayed()
            write_tasks = [
                dask.delayed(write_partition_as_geoparquet)(
                    part,
                    geom_fn,
                    output_dir,
                    partition_col,
                    dggs_col,
                    kwargs.get("compression", "ZSTD"),
                )
                for part in delayed_parts
            ]

            with TqdmCallback(desc="Writing GeoParquet"):
                dask.compute(*write_tasks)

            LOGGER.debug("GeoParquet output writing complete")

    LOGGER.debug("Parent cell partitioning complete")

    return


def polyfill(
    indexer: VectorIndexer,
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
    df = indexer.polyfill(df, resolution)

    if len(df.index) == 0:
        # Conversion resulted in empty output (e.g. large cell, small feature)
        return None

    df.index.rename(f"{indexer.dggs}_{resolution:02}", inplace=True)

    # Secondary (parent) index, used later for partitioning
    df = indexer.secondary_index(df, parent_res)

    df.to_parquet(
        PurePath(output_directory, pq_in.name), engine="auto", compression=compression
    )
    return None


def polyfill_star(args) -> None:
    return polyfill(*args)


def bisection_preparation(
    df: pd.DataFrame,
    dggs: str,
    parent_res: int,
    cut_crs: pyproj.CRS = None,
    cut_threshold: Union[None, float] = None,
) -> tuple[pd.DataFrame, pyproj.CRS, Union[None, float]]:
    cut_threshold = float(cut_threshold) if cut_threshold != None else None

    if cut_threshold and cut_crs:
        if df.crs is None and len(df.index) == 0:
            # empty + naive: nothing to transform
            df = df.set_crs(cut_crs, allow_override=True)
        elif df.crs is None:
            raise ValueError(
                "Input has no CRS; cannot reproject. Specify input CRS or provide a dataset with CRS."
            )
        else:
            df = df.to_crs(cut_crs)
    else:
        cut_crs = df.crs

    if cut_crs is None:
        LOGGER.warning("Input has no defined CRS, and cut_crs is not specified")
    elif cut_threshold != 0:
        LOGGER.debug("Cutting with CRS: %s", df.crs)

    if not cut_crs.is_projected and cut_threshold != 0:
        LOGGER.warning(
            f"CRS {cut_crs} is not a projected coordinate system. (units: {cut_crs.axis_info[0].unit_name}) Bisection will result in sections of varying area"
        )
    elif cut_threshold != 0:
        LOGGER.debug(
            f"Using CRS units for input polygon bisection: {cut_crs.axis_info[0].unit_name}"
        )

    if cut_threshold == None:
        unit_name = cut_crs.axis_info[0].unit_name
        cut_threshold_m2 = const.DEFAULT_AREA_THRESHOLD_M2(dggs, (int(parent_res)))
        if unit_name == "metre":
            cut_threshold = cut_threshold_m2
        elif unit_name == "feet":
            cut_threshold = cut_threshold_m2 * 3.28084
        else:
            cut_threshold = 100000000 if cut_crs.is_projected else 0.5
            LOGGER.warning(
                f'Unspecified cut_threshold for {"projected" if cut_crs.is_projected else "geographic"} CRS: {cut_crs}, with squared units: {unit_name}'
            )
        LOGGER.debug(f"Using default cut_threshold of {cut_threshold} ({unit_name}^2)")

    return df, cut_crs, cut_threshold


def bisect_geometry(geometry, cut_threshold):
    return GeometryCollection(katana.katana(geometry, cut_threshold))


def index(
    dggs: str,
    input_file: Union[Path, str],
    output_directory: Union[Path, str],
    resolution: int,
    parent_res: Union[None, int],
    keep_attributes: bool,
    chunksize: int,
    spatial_sorting: str,
    cut_threshold: Union[None, float],
    processes: int,
    compression: str = "snappy",
    id_field: str = None,
    cut_crs: pyproj.CRS = None,
    con: SQLConnectionType = None,
    layer: str = None,
    geom_col: str = "geom",
    geo: str = const.GeoOutputMode.NONE.value,
    overwrite: bool = False,
    compact: bool = True,
) -> Path:
    """
    Performs multi-threaded DGGS indexing on geometries (including multipart and collections).
    """
    indexer = idxfactory.indexer_instance(dggs)
    parent_res = get_parent_res(dggs, parent_res, resolution)

    if layer and con:
        # Database connection
        with con.connect() as connection:
            query = None
            params = {"layer": layer}

            if keep_attributes:
                query = f"SELECT * FROM {layer}"
            elif id_field and not keep_attributes:
                query = sqlalchemy.text("SELECT :id_field, :geom_col FROM :layer")
                params.update({"id_field": id_field, "geom_col": geom_col})
            else:
                query = sqlalchemy.text("SELECT :geom_col FROM :layer")
                params["geom_col"] = geom_col

            df = gpd.read_postgis(
                query, connection, geom_col=geom_col, params=params
            ).rename_geometry("geometry")
    else:
        # Read file
        df = gpd.read_file(input_file, layer=layer)

    if df is None or len(df.index) == 0:
        LOGGER.warning(
            "Input contained 0 features (layer=%s). Nothing to index; exiting.",
            layer if layer else "<default>",
        )
        return output_directory

    df, cut_crs, cut_threshold = bisection_preparation(
        df, dggs, parent_res, cut_crs, cut_threshold
    )

    if id_field:
        df = df.set_index(id_field)
    else:
        df = df.reset_index()
        df = df.rename(columns={"index": "fid"}).set_index("fid")

    if not keep_attributes:
        # Remove all attributes except the geometry
        df = df.loc[:, ["geometry"]]

    LOGGER.debug("Bisecting large geometries")

    if cut_threshold is not None and cut_threshold > 0:
        with ThreadPoolExecutor(max_workers=max(1, processes)) as executor:
            futures = []
            for index, row in df.iterrows():
                future = executor.submit(bisect_geometry, row.geometry, cut_threshold)
                futures.append((index, future))

            with tqdm(total=len(futures), desc="Bisection") as pbar:
                for index, future in futures:
                    df.at[index, "geometry"] = future.result()
                    pbar.update(1)
    else:
        LOGGER.debug("No bisection applied to input.")

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
        with TqdmCallback(desc="Spatially partitioning"):
            ddf.to_parquet(tmpdir, overwrite=True)

        filepaths = list(map(lambda f: f.absolute(), Path(tmpdir).glob("*")))

        # Multithreaded DGGS indexing
        LOGGER.debug(
            "DGGS indexing by spatial partitions with resolution: %d",
            resolution,
        )
        with tempfile.TemporaryDirectory(suffix=".parquet") as tmpdir2:

            args = [
                (
                    indexer,
                    filepath,
                    spatial_sort_col,
                    resolution,
                    parent_res,
                    tmpdir2,
                    compression,
                )
                for filepath in filepaths
            ]

            with ProcessPoolExecutor(max_workers=processes) as executor:
                futures = {executor.submit(polyfill_star, arg): arg for arg in args}

                for future in tqdm(
                    as_completed(futures), total=len(futures), desc="DGGS indexing"
                ):
                    try:
                        future.result()
                    except Exception as e:
                        LOGGER.error(f"Task failed with {e}")
                        raise (e)

            parent_partitioning(
                indexer,
                Path(tmpdir2),
                output_directory,
                resolution,
                parent_res,
                id_field,
                compact,
                geo,
                overwrite=overwrite,
                compression=compression,
            )

    return output_directory
