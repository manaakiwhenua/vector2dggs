import errno
import json
import logging
import multiprocessing
import os
import shutil
import tempfile
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from pathlib import Path, PurePath
from types import ModuleType
from uuid import uuid4

import antimeridian
import click
import click_log
import dask
import dask.dataframe as dd
import dask_geopandas as dgpd
import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as pa_ds
import pyarrow.parquet as pq
import pyproj
import shapely
import shapely.affinity
import sqlalchemy
from shapely.geometry import GeometryCollection
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError, NoSuchModuleError
from tqdm import tqdm
from tqdm.dask import TqdmCallback

import vector2dggs.constants as const
import vector2dggs.indexerfactory as idxfactory
from vector2dggs.indexers.vectorindexer import VectorIndexer

from . import katana

resource: ModuleType | None
try:
    import resource
except ImportError:  # resource is POSIX-only
    resource = None

SQLConnectionType = sqlalchemy.engine.Engine


LOGGER = logging.getLogger(__name__)
click_log.basic_config(LOGGER)
click_log.ColorFormatter.colors["info"] = {"fg": "green"}


class ParentResolutionException(Exception):
    pass


class IdFieldError(ValueError):
    """Raised when an invalid or missing ID field is provided."""

    pass


def check_resolutions(resolution: str | int, parent_res: None | str | int) -> None:
    if parent_res is not None and not int(parent_res) < int(resolution):
        raise ParentResolutionException(
            f"Parent resolution ({parent_res}) must be less than target resolution ({resolution})"
        )


def check_compaction_requirements(compact: bool, id_field: str | None) -> None:
    if compact and not id_field:
        raise IdFieldError(
            "An id_field is required for compaction, in order to handle the potential for overlapping features"
        )


def validate_compression(ctx, param, value: str) -> str:
    """
    Click callback that fails fast on an unsupported Parquet compression
    codec, by asking pyarrow directly (an in-memory, zero-row write) rather
    than maintaining a hardcoded codec list that could drift from what the
    installed pyarrow actually supports.
    """
    try:
        pq.write_table(pa.table({"_": [0]}), pa.BufferOutputStream(), compression=value)
    except Exception as e:
        raise click.BadParameter(
            f"'{value}' is not a supported Parquet compression codec: {e}"
        ) from e
    return value


def db_conn_and_input_path(
    vector_input: str | Path,
) -> tuple[SQLConnectionType | None, str | Path]:
    if Path(vector_input).exists():
        return (None, Path(vector_input))

    try:
        url = make_url(str(vector_input))
        url.get_dialect()
    except (ArgumentError, NoSuchModuleError):
        pass
    else:
        return (sqlalchemy.create_engine(url), vector_input)

    if "://" in str(vector_input):
        # e.g. https:// or s3://; GDAL may be able to read it
        return (None, str(vector_input))

    LOGGER.error(
        f"Input vector {vector_input} does not exist, and is not recognised as a remote URI"
    )
    raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), vector_input)


def resolve_output_path(output_directory: str | Path, overwrite: bool) -> str | Path:
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


def get_parent_res(dggs: str, parent_res: None | str | int, resolution: int) -> int:
    """
    Uses a parent resolution,
    OR,
    Given a target resolution, returns our recommended parent resolution.

    Used for intermediate re-partioning.
    """
    if dggs not in const.DEFAULT_DGGS_PARENT_RES:
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


def raise_rlimit_nofile() -> None:
    """Raise the soft RLIMIT_NOFILE to the hard limit (POSIX; no-op elsewhere)."""
    if resource is None:
        return
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        if soft != hard:
            resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
            LOGGER.debug("Raised RLIMIT_NOFILE soft limit from %s to %s", soft, hard)
    except (ValueError, OSError) as e:
        LOGGER.debug("Could not raise RLIMIT_NOFILE: %s", e)


def write_partition(
    partition_df: pd.DataFrame,
    geo_serialisation_method,
    output_dir: Path,
    partition_col: str,
    dggs_col: str,
    compression: str,
) -> int:
    """
    Hive-partitioned parquet write of one dask partition; GeoParquet when
    geo_serialisation_method (cell -> shapely geometry) is given.
    """
    if partition_df.empty:
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

    # Build shapely geometries for this dask partition.
    # Drop rows with null DGGS cell IDs to avoid serialisation failures.
    cell_ids = (
        partition_df[dggs_col]
        if dggs_col in partition_df.columns
        else partition_df.index.to_series(index=partition_df.index)
    )
    valid_cell_mask = pd.notna(cell_ids)
    if not bool(valid_cell_mask.any()):
        return 0
    if not bool(valid_cell_mask.all()):
        partition_df = partition_df.loc[valid_cell_mask].copy()
        cell_ids = cell_ids.loc[valid_cell_mask]

    pdf = partition_df.copy()
    pdf[partition_col] = pdf[partition_col].astype("string")
    if geo_serialisation_method is not None:
        pdf["geometry"] = shapely.to_wkb(
            cell_ids.map(geo_serialisation_method), hex=False
        )
    # sorted by partition value: one file per parent cell, no writer churn
    pdf = pdf.sort_values(partition_col, kind="stable")

    table = pa.Table.from_pandas(pdf, preserve_index=True)
    if geo_serialisation_method is not None:
        table = _with_geoparquet_metadata(table)

    # Explicitly type the partition column as string so that Hive directory values
    # like "204" (valid geohash) or "9983180000000000" (A5 cell ID) are not
    # inferred as integers by PyArrow readers.
    partitioning = pa_ds.partitioning(
        pa.schema([(partition_col, pa.string())]), flavor="hive"
    )
    pq.write_to_dataset(
        table,
        root_path=str(output_dir),
        partitioning=partitioning,
        compression=compression,
        basename_template=f"part.{{i}}-{uuid4().hex}.parquet",
        use_threads=True,
        max_open_files=const.MAX_OPEN_FILES_PER_TASK,
        # pyarrow's default max_partitions (1024) is a safety valve; this
        # batch's true partition count is known, so pass it exactly
        max_partitions=max(1, int(pdf[partition_col].nunique())),
    )

    return int(len(pdf.index) > 0)


def _with_geoparquet_metadata(table: pa.Table) -> pa.Table:
    """Binary-typed geometry column plus GeoParquet 1.1.0 metadata."""
    geom_idx = table.schema.get_field_index("geometry")
    if not (
        pa.types.is_binary(table.field(geom_idx).type)
        or pa.types.is_large_binary(table.field(geom_idx).type)
    ):
        geom_array = pa.array(table.column(geom_idx).to_pylist(), type=pa.binary())
        table = table.set_column(geom_idx, "geometry", geom_array)

    geoms = shapely.from_wkb(table.column(geom_idx).to_numpy(zero_copy_only=False))
    valid = pd.notna(geoms)
    if valid.any():
        valid[valid] &= ~shapely.is_empty(geoms[valid])
    col_meta = {
        "encoding": "WKB",
        "crs": pyproj.CRS.from_epsg(4326).to_json_dict(),
    }
    if valid.any():
        bounds = np.atleast_2d(np.asarray(shapely.bounds(geoms[valid])))
        bounds = bounds[~np.isnan(bounds).any(axis=1)]
        if len(bounds):
            col_meta["bbox"] = [
                float(bounds[:, 0].min()),
                float(bounds[:, 1].min()),
                float(bounds[:, 2].max()),
                float(bounds[:, 3].max()),
            ]
        col_meta["geometry_types"] = sorted({g.geom_type for g in geoms[valid]})

    geo_meta = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {"geometry": col_meta},
    }
    return table.replace_schema_metadata(
        {**(table.schema.metadata or {}), b"geo": json.dumps(geo_meta).encode("utf-8")}
    )


def _merge_partition_files(partition_dir: Path, compression: str) -> None:
    """
    Merges all Parquet files within a single hive partition directory into one file.
    Preserves and correctly aggregates GeoParquet 'geo' metadata (bbox, geometry_types)
    if present. Peak memory is bounded to one parent cell's data at a time.
    """
    files = sorted(partition_dir.glob("*.parquet"))
    if len(files) <= 1:
        return

    # partitioning=None: don't hive-parse the file's own path into a column
    tables = [pq.read_table(f, partitioning=None) for f in files]

    # Aggregate 'geo' metadata across files if present (GeoParquet)
    all_bboxes = []
    all_geometry_types = set()
    base_geo_meta = None
    for t in tables:
        raw = (t.schema.metadata or {}).get(b"geo")
        if raw:
            geo_info = json.loads(raw)
            if base_geo_meta is None:
                base_geo_meta = geo_info
            col = geo_info.get("columns", {}).get("geometry", {})
            if "bbox" in col:
                all_bboxes.append(col["bbox"])
            all_geometry_types.update(col.get("geometry_types", []))

    # Build a unified schema: for each field, prefer large_string over string so
    # that all partitions (which may have written either variant) can be cast cleanly.
    unified_schema = tables[0].schema
    for t in tables[1:]:
        unified_schema = pa.unify_schemas(
            [unified_schema, t.schema], promote_options="permissive"
        )
    # Normalise string / large_string mismatches to large_string
    unified_fields = []
    for field in unified_schema:
        if pa.types.is_string(field.type):
            unified_fields.append(field.with_type(pa.large_utf8()))
        else:
            unified_fields.append(field)
    unified_schema = pa.schema(unified_fields, metadata=unified_schema.metadata)

    tables = [t.cast(unified_schema) for t in tables]
    table = pa.concat_tables(tables)

    if base_geo_meta is not None:
        col_meta = base_geo_meta["columns"]["geometry"].copy()
        if all_bboxes:
            col_meta["bbox"] = [
                min(b[0] for b in all_bboxes),
                min(b[1] for b in all_bboxes),
                max(b[2] for b in all_bboxes),
                max(b[3] for b in all_bboxes),
            ]
        if all_geometry_types:
            col_meta["geometry_types"] = sorted(all_geometry_types)
        base_geo_meta["columns"]["geometry"] = col_meta
        new_meta = {
            **(table.schema.metadata or {}),
            b"geo": json.dumps(base_geo_meta).encode("utf-8"),
        }
        table = table.replace_schema_metadata(new_meta)

    merged = partition_dir / f"part.0-{uuid4().hex}.parquet"
    pq.write_table(table, merged, compression=compression)
    for f in files:
        f.unlink()


def _parent_partitioning(
    indexer: VectorIndexer,
    input_dir: Path,
    output_dir: Path | str,
    resolution: int,
    parent_res: int,
    id_field: str | None,
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
            # Shuffle by parent cell first: compaction is per-partition, and
            # the parent_res floor means siblings can only merge within one
            # parent cell.
            ddf = (
                ddf.reset_index(drop=False)
                .dropna(subset=[partition_col])
                .shuffle(on=partition_col)
                .map_partitions(
                    indexer.compaction,
                    resolution,
                    meta.columns.to_list(),  # Column order to be returned
                    dggs_col,
                    id_field,
                    parent_res,
                    meta=meta,
                )
            )

        if geo == const.GeoOutputMode.NONE.value:
            geom_fn = None
        elif geo == const.GeoOutputMode.POINT.value:
            geom_fn = indexer.cell_to_point
        elif geo == const.GeoOutputMode.POLYGON.value:
            geom_fn = indexer.cell_to_polygon
        else:
            raise ValueError(
                f"Unknown geo output mode '{geo}'. Expected one of {const.GEOM_TYPES}."
            )

        write_tasks = [
            dask.delayed(write_partition)(
                part,
                geom_fn,
                output_dir,
                partition_col,
                dggs_col,
                kwargs.get("compression", "ZSTD"),
            )
            for part in ddf.to_delayed()
        ]
        with TqdmCallback(desc="Writing output"):
            dask.compute(*write_tasks)

        # Combine multiple parts per parent partition into one file per parent partition, to avoid too many small files and to correctly aggregate GeoParquet metadata; this is helpful for cloud object storage and spatial databases, which often perform better with fewer files
        merge_tasks = [
            dask.delayed(_merge_partition_files)(d, kwargs.get("compression", "ZSTD"))
            for d in sorted(Path(output_dir).iterdir())
            if d.is_dir()
        ]
        with TqdmCallback(desc="Merging to one file per parent cell"):
            dask.compute(*merge_tasks)

        LOGGER.debug("GeoParquet output writing complete")

    LOGGER.debug("Parent cell partitioning complete")


def _polyfill(
    indexer: VectorIndexer,
    pq_in: Path,
    spatial_sort_col: str,
    resolution: int,
    parent_res: int,
    output_directory: str,
    compression: str,
    id_col: str,
) -> np.ndarray:
    """
    Reads a geoparquet, performs polyfilling (for Polygon),
    linetracing (for LineString), or indexing (for Point),
    and writes out to parquet. Returns the ids of features that
    produced at least one cell.
    """
    df = gpd.read_parquet(pq_in).reset_index()
    if spatial_sort_col != "none":
        df = df.drop(columns=[spatial_sort_col])
    if df.empty:
        return np.array([])

    # DGGS specific conversion
    df = indexer.polyfill(df, resolution)

    if df.empty:
        # e.g. features smaller than a cell at this resolution
        return np.array([])

    df.index.rename(f"{indexer.dggs}_{resolution:02}", inplace=True)

    # Secondary (parent) index, used later for partitioning
    df = indexer.secondary_index(df, parent_res)

    df.to_parquet(
        PurePath(output_directory, pq_in.name), engine="auto", compression=compression
    )
    return df[id_col].unique()


def _polyfill_star(args) -> np.ndarray:
    return _polyfill(*args)


def bisection_preparation(
    df: pd.DataFrame,
    dggs: str,
    resolution: int,
    cut_crs: pyproj.CRS | None = None,
    cut_threshold: None | float = None,
) -> tuple[pd.DataFrame, pyproj.CRS, None | float]:
    cut_threshold = float(cut_threshold) if cut_threshold is not None else None

    # cut_threshold == 0 disables bisection entirely, ignoring cut_crs
    if cut_crs is not None and cut_threshold != 0:
        if df.crs is None and df.empty:
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
        raise ValueError(
            "Input has no CRS, which is required for indexing. "
            "Provide a dataset with a defined CRS."
        )

    if cut_threshold is None:
        axis = cut_crs.axis_info[0]
        # unit_conversion_factor: linear units -> metres, angular -> radians
        metres_per_unit = axis.unit_conversion_factor * (
            1 if cut_crs.is_projected else const.EARTH_MEAN_RADIUS_M
        )
        cut_threshold_m2 = const.DEFAULT_AREA_THRESHOLD_M2(dggs, int(resolution))
        cut_threshold = cut_threshold_m2 / metres_per_unit**2
        LOGGER.debug(
            f"Using default cut_threshold of {cut_threshold} ({axis.unit_name}^2)"
        )

    return df, cut_crs, cut_threshold


def bisect_geometry(geometry, cut_threshold):
    return GeometryCollection(katana.katana(geometry, cut_threshold))


def _read_input(
    input_file: Path | str,
    layer: str | None,
    con: SQLConnectionType | None,
    keep_attributes: bool,
    id_field: str | None,
    geom_col: str,
) -> gpd.GeoDataFrame:
    if layer and con:
        with con.connect() as connection:
            parts = layer.rsplit(".", 1)
            schema, tbl_name = (
                (parts[0], parts[1]) if len(parts) == 2 else (None, parts[0])
            )
            tbl = sqlalchemy.Table(
                tbl_name,
                sqlalchemy.MetaData(),
                schema=schema,
                autoload_with=connection,
            )
            if keep_attributes:
                stmt = tbl.select()
            elif id_field and not keep_attributes:
                stmt = sqlalchemy.select(tbl.c[id_field], tbl.c[geom_col])
            else:
                stmt = sqlalchemy.select(tbl.c[geom_col])
            result = gpd.read_postgis(stmt, connection, geom_col=geom_col)
            if geom_col != "geometry":
                result = result.rename_geometry("geometry")
            return result
    return gpd.read_file(input_file, layer=layer)


def _prepare_dataframe(
    df: gpd.GeoDataFrame,
    id_field: str | None,
    keep_attributes: bool,
) -> gpd.GeoDataFrame:
    if id_field:
        df = df.set_index(id_field)
    else:
        df = df.reset_index()
        df = df.rename(columns={"index": "fid"}).set_index("fid")
    if not keep_attributes:
        df = df.loc[:, ["geometry"]]
    return df


def _run_bisection(
    df: gpd.GeoDataFrame,
    cut_threshold: None | float,
    processes: int,
) -> gpd.GeoDataFrame:
    LOGGER.debug("Bisecting large geometries")
    if cut_threshold is not None and cut_threshold > 0:
        # katana's own early exit is exactly this bbox-area check; computing
        # it vectorized upfront (rather than dispatching every row to the
        # thread pool regardless) skips that overhead for features that
        # don't need bisecting at all.
        bounds = df.geometry.bounds
        bbox_area = (bounds["maxx"] - bounds["minx"]) * (
            bounds["maxy"] - bounds["miny"]
        )
        # Positions (not index labels) of oversized rows: the index is the
        # user-supplied id_field, which is not guaranteed to be unique, and
        # label-based assignment would write one feature's result into every
        # row sharing that label.
        oversized_positions = np.flatnonzero(bbox_area > cut_threshold)
        LOGGER.debug(
            "%d of %d features exceed the bisection area threshold",
            len(oversized_positions),
            len(df),
        )
        if len(oversized_positions):
            geometry_loc = df.columns.get_loc("geometry")
            with ThreadPoolExecutor(max_workers=max(1, processes)) as executor:
                futures = [
                    (
                        pos,
                        executor.submit(
                            bisect_geometry, df.geometry.iloc[pos], cut_threshold
                        ),
                    )
                    for pos in oversized_positions
                ]
                with tqdm(total=len(futures), desc="Bisection") as pbar:
                    for pos, future in futures:
                        df.iloc[pos, geometry_loc] = future.result()
                        pbar.update(1)
    else:
        LOGGER.debug("No bisection applied to input.")
    return df


def _crosses_antimeridian(geom) -> bool:
    """
    Heuristically detect whether a geometry's naive bounding box suggests it
    spans the antimeridian: a geometry that genuinely crosses it produces an
    implausibly wide (> 180 degree) longitude span, since its vertices sit
    close to -180 and +180 rather than wrapping through the boundary.
    """
    if geom is None or geom.is_empty:
        return False
    minx, _, maxx, _ = geom.bounds
    return (maxx - minx) > 180


def _fix_antimeridian_crossing(geom):
    """
    Split a geometry that spans the antimeridian into the equivalent
    correctly-wound multi-part geometry. Geometries that don't actually
    cross the antimeridian are returned unchanged.
    """
    if geom is None or geom.is_empty:
        return geom
    if geom.geom_type == "GeometryCollection":
        return GeometryCollection([_fix_antimeridian_crossing(g) for g in geom.geoms])
    if not _crosses_antimeridian(geom):
        return geom
    if geom.geom_type == "Polygon":
        return antimeridian.fix_polygon(geom)
    elif geom.geom_type == "MultiPolygon":
        return antimeridian.fix_multi_polygon(geom)
    elif geom.geom_type == "LineString":
        return antimeridian.fix_line_string(geom, great_circle=True)
    elif geom.geom_type == "MultiLineString":
        return antimeridian.fix_multi_line_string(geom, great_circle=True)
    return geom


def _normalise_longitudes(df: gpd.GeoDataFrame) -> tuple[gpd.GeoDataFrame, bool]:
    """
    Shift geometries stored with longitudes beyond +/-180 (e.g. the Chatham
    Islands at 183 degrees E) back into range. Geometries straddling 180 are
    wrapped coordinate-wise, leaving a genuine antimeridian crossing for
    downstream handling. Returns (df, whether any straddlers were wrapped).
    """
    if df.crs is None or not df.crs.is_geographic:
        raise ValueError("_normalise_longitudes requires a geographic CRS")
    bounds = df.geometry.bounds
    east = bounds["minx"] >= 180
    west = bounds["maxx"] <= -180
    straddle = ~east & ~west & ((bounds["maxx"] > 180) | (bounds["minx"] < -180))
    n = int(east.sum() + west.sum() + straddle.sum())
    if n:
        LOGGER.info("Normalising longitudes for %d features stored beyond +/-180", n)
    for mask, xoff in ((east, -360), (west, 360)):
        if mask.any():
            df.loc[mask, "geometry"] = df.loc[mask, "geometry"].apply(
                lambda g, xoff=xoff: shapely.affinity.translate(g, xoff=xoff)
            )
    if straddle.any():
        df.loc[straddle, "geometry"] = df.loc[straddle, "geometry"].apply(
            lambda g: shapely.transform(
                g, lambda c: np.column_stack((((c[:, 0] + 180) % 360) - 180, c[:, 1]))
            )
        )
    return df, bool(straddle.any())


def _clean_geometries(df: gpd.GeoDataFrame, indexer: VectorIndexer) -> gpd.GeoDataFrame:
    LOGGER.debug("Exploding geometry collections and multipolygons")
    # Correct antimeridian-crossing artifacts when the source coordinates were
    # unambiguous (projected CRS, or unwrapped longitudes), and only for
    # backends whose polyfill isn't already geodesic.
    was_projected = df.crs is not None and not df.crs.is_geographic
    df = df.to_crs(4326)
    df, had_unwrapped_crossing = _normalise_longitudes(df)
    if (was_projected or had_unwrapped_crossing) and not indexer.GEODESIC_POLYFILL:
        LOGGER.debug("Correcting antimeridian-crossing geometries")
        df["geometry"] = df.geometry.apply(_fix_antimeridian_crossing)
    df = (
        df.explode(index_parts=False).explode(  # Explode from GeometryCollection
            index_parts=False
        )  # Explode multipolygons to polygons
    ).reset_index()
    df = drop_condition(
        df,
        df[df.geometry.is_empty | df.geometry.isna()].index,
        "Considering empty or null geometries",
    )
    df = drop_condition(
        df,
        df[
            (df.geometry.geom_type != "Polygon")
            & (df.geometry.geom_type != "LineString")
            & (df.geometry.geom_type != "Point")
        ].index,
        "Considering unsupported geometries",
    )
    return df


def _mp_context() -> multiprocessing.context.BaseContext:
    """Never fork: the parent is multi-threaded by the time the pool starts."""
    methods = multiprocessing.get_all_start_methods()
    return multiprocessing.get_context(
        "forkserver" if "forkserver" in methods else "spawn"
    )


def _run_dggs_indexing(
    indexer: VectorIndexer,
    filepaths: list,
    spatial_sort_col: str,
    resolution: int,
    parent_res: int,
    output_dir: str,
    compression: str,
    processes: int,
    id_col: str,
) -> set:
    LOGGER.debug("DGGS indexing by spatial partitions with resolution: %d", resolution)
    args = [
        (
            indexer,
            filepath,
            spatial_sort_col,
            resolution,
            parent_res,
            output_dir,
            compression,
            id_col,
        )
        for filepath in filepaths
    ]
    indexed_ids: set = set()
    with ProcessPoolExecutor(
        max_workers=max(1, processes), mp_context=_mp_context()
    ) as executor:
        futures = {executor.submit(_polyfill_star, arg): arg for arg in args}
        for future in tqdm(
            as_completed(futures), total=len(futures), desc="DGGS indexing"
        ):
            try:
                indexed_ids.update(future.result())
            except Exception as e:
                LOGGER.error(f"Task failed with {e}")
                raise e
    return indexed_ids


def index(
    dggs: str,
    input_file: Path | str,
    output_directory: Path | str,
    resolution: int,
    parent_res: None | str | int,
    keep_attributes: bool,
    chunksize: int,
    spatial_sorting: str,
    cut_threshold: None | float,
    processes: int,
    compression: str = "snappy",
    id_field: str | None = None,
    cut_crs: pyproj.CRS | None = None,
    con: SQLConnectionType | None = None,
    layer: str | None = None,
    geom_col: str = "geom",
    geo: str = const.GeoOutputMode.NONE.value,
    overwrite: bool = False,
    compact: bool = False,
) -> Path | str:
    """
    Performs multi-threaded DGGS indexing on geometries (including multipart and collections).
    """
    check_compaction_requirements(compact, id_field)
    indexer = idxfactory.indexer_instance(dggs)
    parent_res = get_parent_res(dggs, parent_res, resolution)

    df = _read_input(input_file, layer, con, keep_attributes, id_field, geom_col)
    if df is None or df.empty:
        LOGGER.warning(
            "Input contained 0 features (layer=%s). Nothing to index; exiting.",
            layer if layer else "<default>",
        )
        return output_directory
    if df.crs is None:
        raise ValueError(
            "Input has no CRS, which is required for indexing. "
            "Provide a dataset with a defined CRS."
        )

    df, cut_crs, cut_threshold = bisection_preparation(
        df, dggs, resolution, cut_crs, cut_threshold
    )
    df = _prepare_dataframe(df, id_field, keep_attributes)
    features_in = set(df.index)
    df = _run_bisection(df, cut_threshold, processes)
    df = _clean_geometries(df, indexer)

    ddf = dgpd.from_geopandas(df, chunksize=max(1, chunksize), sort=True)
    if spatial_sorting != "none":
        LOGGER.debug("Spatially sorting and partitioning (%s)", spatial_sorting)
        ddf = ddf.spatial_shuffle(by=spatial_sorting)
    spatial_sort_col = (
        spatial_sorting
        if spatial_sorting in ("geohash", "none")
        else f"{spatial_sorting}_distance"
    )

    with tempfile.TemporaryDirectory(suffix=".parquet") as tmpdir:
        with TqdmCallback(desc="Spatially partitioning"):
            ddf.to_parquet(tmpdir, overwrite=True)
        filepaths = [f.absolute() for f in Path(tmpdir).glob("*")]

        with tempfile.TemporaryDirectory(suffix=".parquet") as tmpdir2:
            indexed_ids = _run_dggs_indexing(
                indexer,
                filepaths,
                spatial_sort_col,
                resolution,
                parent_res,
                tmpdir2,
                compression,
                processes,
                id_field or "fid",
            )
            dropped = features_in - indexed_ids
            if dropped:
                LOGGER.warning(
                    "%d of %d features produced no cells at resolution %s and were omitted",
                    len(dropped),
                    len(features_in),
                    resolution,
                )
            if not any(Path(tmpdir2).glob("*.parquet")):
                LOGGER.warning(
                    "No features were indexed (resolution %s may be too coarse for the input). Nothing to write; exiting.",
                    resolution,
                )
                return output_directory

            _parent_partitioning(
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
