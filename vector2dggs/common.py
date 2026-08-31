import json
import logging
import math
import multiprocessing
import shutil
import tempfile
from collections.abc import Iterator
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from pathlib import Path, PurePath
from types import ModuleType
from uuid import uuid4

import antimeridian
import click
import click_log
import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as pa_ds
import pyarrow.parquet as pq
import pyogrio
import pyproj
import shapely
import shapely.affinity
import sqlalchemy
from shapely.geometry import GeometryCollection, LineString
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError, NoSuchModuleError
from tqdm import tqdm

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


class UnknownAttributeError(ValueError):
    """Raised when -ka/--keep_attribute names a column not in the input."""

    pass


class CellIdError(ValueError):
    """Raised when --cell-id uint64 is requested for a string-only DGGS."""

    pass


def check_cell_id(cell_id: str, indexer: VectorIndexer) -> None:
    if (
        cell_id == const.CellIdMode.UINT64.value
        and pa.string() == indexer.CELL_ARROW_TYPE
    ):
        raise CellIdError(
            f"--cell-id uint64 is not supported for '{indexer.dggs}': its cell IDs "
            "are strings with no integer form. Only 'string' is available."
        )


def check_resolutions(resolution: str | int, parent_res: None | str | int) -> None:
    if parent_res is not None and not int(parent_res) < int(resolution):
        raise ParentResolutionException(
            f"Parent resolution ({parent_res}) must be less than target resolution ({resolution})"
        )


def check_requested_attributes(
    keep_attribute: tuple[str, ...],
    input_file: Path | str,
    layer: str | None,
    con: SQLConnectionType | None,
) -> None:
    if not keep_attribute:
        return
    if layer and con:
        with con.connect() as connection:
            available = set(_db_table(connection, layer).columns.keys())
    else:
        available = set(pyogrio.read_info(str(input_file), layer=layer)["fields"])
    unknown = [c for c in keep_attribute if c not in available]
    if unknown:
        raise UnknownAttributeError(
            f"Unknown attribute(s) for -ka/--keep_attribute: {', '.join(unknown)}. "
            f"Available: {', '.join(sorted(available))}"
        )


def _fid_column(input_file: Path | str, layer: str | None) -> str | None:
    """
    The name of the file's FID/primary-key slot, e.g. "fid" for a plain
    GPKG, or a dataset-specific name for a Kart working copy that promotes
    its own PK there. Returns None if the format has no FID concept, or
    the file has none set.
    """
    return pyogrio.read_info(str(input_file), layer=layer).get("fid_column") or None


def check_id_field(
    id_field: str | None,
    input_file: Path | str,
    layer: str | None,
    con: SQLConnectionType | None,
) -> None:
    if not id_field:
        return
    if layer and con:
        with con.connect() as connection:
            available = set(_db_table(connection, layer).columns.keys())
    else:
        info = pyogrio.read_info(str(input_file), layer=layer)
        available = set(info["fields"]) | {info.get("fid_column")}
    if id_field not in available:
        raise IdFieldError(
            f"Unknown -id/--id_field '{id_field}'. "
            f"Available: {', '.join(sorted(c for c in available if c))}"
        )


def resolve_layer(
    input_file: Path | str,
    layer: str | None,
    con: SQLConnectionType | None = None,
) -> str | None:
    """
    When no layer is requested and the input has several, GDAL would silently
    index the first; announce that choice in the tool's own voice and pin the
    layer explicitly so pyogrio's per-call UserWarning never fires.
    """
    if layer is not None or con is not None:
        return layer
    layers = pyogrio.list_layers(str(input_file))
    if len(layers) > 1:
        names = [name for name, _ in layers]
        LOGGER.warning(
            "Input has %d layers: %s. Indexing '%s' (the default). "
            "Pass -l/--layer to choose a different layer.",
            len(names),
            ", ".join(f"'{name}'" for name in names),
            names[0],
        )
        return names[0]
    return layer


def resolve_default_id_field(
    input_file: Path | str,
    layer: str | None,
    con: SQLConnectionType | None,
) -> str | None:
    """
    When -id/--id_field isn't given, prefer a real internal ID over a
    constructed 0...n sequence: a file's physically-stored FID column, or
    a DB table's single-column primary key. Falls back to None (the
    caller's synthetic-sequence path) with a warning when neither is
    available, e.g. Shapefile, or a composite/missing DB primary key.
    """
    if layer and con:
        with con.connect() as connection:
            pk_cols = list(_db_table(connection, layer).primary_key.columns)
        if len(pk_cols) == 1:
            return pk_cols[0].name
    else:
        fid_col = _fid_column(input_file, layer)
        if fid_col:
            return fid_col
    LOGGER.warning(
        "No internal ID found (no physically-stored FID column, or no "
        "single-column primary key); using a constructed 0...n index tied "
        "to row position in the read order, which won't match a different "
        "export/copy of this data. Pass -id/--id_field to use a specific "
        "field instead."
    )
    return None


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
        try:
            return (sqlalchemy.create_engine(url), vector_input)
        except ModuleNotFoundError as e:
            raise ImportError(
                f"Database input requires the '{e.name}' driver; for "
                "PostgreSQL/PostGIS: pip install vector2dggs[postgres]"
            ) from e

    # Remote URI or GDAL virtual path: raises DataSourceError if GDAL can't open it
    pyogrio.read_info(str(vector_input))
    return (None, str(vector_input))


def resolve_output_path(output_directory: str | Path, overwrite: bool) -> Path:
    output_directory = Path(output_directory)
    if output_directory.exists() and not overwrite:
        raise FileExistsError(
            f"{output_directory} already exists; if you want to overwrite this, use the -o/--overwrite flag"
        )
    return output_directory


def _commit_output(staging: Path, output_directory: Path, overwrite: bool) -> Path:
    """
    Move the staged run into place. The previous output (if any) is renamed
    aside before the swap and only deleted afterwards, so no crash window
    destroys data; each placement is a single same-filesystem rename.

    overwrite is re-checked here: validation at run start doesn't authorise
    replacing a directory that appeared during the run.
    """
    if output_directory.exists():
        if not overwrite:
            raise FileExistsError(
                f"{output_directory} was created while this run was in progress; "
                "not replacing it without -o/--overwrite"
            )
        LOGGER.warning(f"Overwriting the contents of {output_directory}")
        replaced = output_directory.parent / f".{output_directory.name}.replaced"
        if replaced.exists():
            shutil.rmtree(replaced)
        output_directory.rename(replaced)
        staging.rename(output_directory)
        shutil.rmtree(replaced)
    else:
        staging.rename(output_directory)
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
    indexer: VectorIndexer,
    cell_id: str,
    compact: bool,
) -> int:
    """
    Hive-partitioned parquet write of one dataframe; GeoParquet when
    geo_serialisation_method (cell -> shapely geometry) is given.

    May mutate partition_df in place (columns added/retyped, rows dropped,
    row order changed) -- callers that need the original afterward must
    copy it themselves first.
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
        partition_df = partition_df.loc[valid_cell_mask]
        cell_ids = cell_ids.loc[valid_cell_mask]

    # Geometry is built from the working (native, for capable backends) cell
    # form, before any string conversion below.
    if geo_serialisation_method is not None:
        partition_df["geometry"] = shapely.to_wkb(
            cell_ids.map(geo_serialisation_method), hex=False
        )

    # The parent/partition column decides the hive directory name, created
    # permanently by this write - even under compaction, the later merge
    # step only rewrites file contents within an existing directory, never
    # directory names - so it always follows the final requested cell_id
    # mode, regardless of compact.
    emit_string = cell_id == const.CellIdMode.STRING.value
    if emit_string and pa.string() != indexer.CELL_ARROW_TYPE:
        partition_df[partition_col] = indexer.cells_to_string(
            partition_df[partition_col]
        )
    partition_arrow_type = pa.string() if emit_string else indexer.CELL_ARROW_TYPE
    partition_df[partition_col] = partition_df[partition_col].astype(
        "string" if emit_string else "uint64"
    )

    # The fine cell column/index defers to the merge step under compaction
    # (mirroring geo_serialisation_method's own compact-gating in _polyfill):
    # compaction needs the native form to correctly group sibling cells, so
    # no conversion happens here when compacting.
    if not compact and emit_string and pa.string() != indexer.CELL_ARROW_TYPE:
        if dggs_col in partition_df.columns:
            partition_df[dggs_col] = indexer.cells_to_string(partition_df[dggs_col])
        else:
            partition_df.index = pd.Index(
                indexer.cells_to_string(partition_df.index),
                name=partition_df.index.name,
            )

    # sorted by partition value: one file per parent cell, no writer churn.
    # Sorting the native form when possible (rather than after string
    # conversion) is a cheap numeric sort instead of a string one; either
    # achieves the same churn-avoidance property, which only needs equal
    # values to end up contiguous, not any particular relative order.
    partition_df.sort_values(partition_col, kind="stable", inplace=True)

    table = pa.Table.from_pandas(partition_df, preserve_index=True)
    if geo_serialisation_method is not None:
        table = _with_geoparquet_metadata(table)

    # Explicitly type the partition column - never let PyArrow infer it from
    # the directory name text, which would misread e.g. a numeric-looking
    # geohash ("204") or a decimal uint64 parent id as some other type.
    partitioning = pa_ds.partitioning(
        pa.schema([(partition_col, partition_arrow_type)]), flavor="hive"
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
        max_partitions=max(1, int(partition_df[partition_col].nunique())),
    )

    return int(len(partition_df.index) > 0)


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


def _geom_fn(indexer: VectorIndexer, geo: str):
    if geo == const.GeoOutputMode.NONE.value:
        return None
    if geo == const.GeoOutputMode.POINT.value:
        return indexer.cell_to_point
    if geo == const.GeoOutputMode.POLYGON.value:
        return indexer.cell_to_polygon
    raise ValueError(
        f"Unknown geo output mode '{geo}'. Expected one of {const.GEOM_TYPES}."
    )


def _merge_partition_files(
    partition_dir: Path,
    compression: str,
    indexer: VectorIndexer | None = None,
    resolution: int | None = None,
    parent_res: int | None = None,
    id_field: str | None = None,
    geo: str | None = None,
    cell_id: str = const.CellIdMode.STRING.value,
) -> None:
    """
    Merges all Parquet files within a single hive partition directory into one
    file. Preserves and correctly aggregates GeoParquet 'geo' metadata (bbox,
    geometry_types) if present. When a compactor is given, the merged rows are
    compacted here — the hive write already routes a parent cell's every row
    into this directory, and the resolution floor stops compaction crossing a
    parent boundary, so each directory is a complete, independent unit — and
    cell geometries are (re)generated for the compacted cells. Peak memory is
    bounded to one parent cell's data at a time.
    """
    compacting = indexer is not None
    files = sorted(partition_dir.glob("*.parquet"))
    if not files or (len(files) <= 1 and not compacting):
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

    # Build a unified schema: for each field, prefer the wider/safer variant so
    # that all partitions (which may disagree) can be cast cleanly without
    # loss - large_string over string, and uint64 over int64 (a non-negative
    # int64 value always fits losslessly in uint64, not vice versa; pyarrow's
    # own "permissive" unification silently picks int64 for this exact
    # mismatch, which would corrupt a genuine uint64 cell ID above 2**63-1).
    per_field_types: dict[str, set[pa.DataType]] = {}
    for t in tables:
        for field in t.schema:
            per_field_types.setdefault(field.name, set()).add(field.type)

    unified_schema = tables[0].schema
    for t in tables[1:]:
        unified_schema = pa.unify_schemas(
            [unified_schema, t.schema], promote_options="permissive"
        )
    unified_fields = []
    for field in unified_schema:
        seen = per_field_types.get(field.name, set())
        if pa.types.is_string(field.type):
            unified_fields.append(field.with_type(pa.large_utf8()))
        elif {pa.int64(), pa.uint64()} <= seen:
            unified_fields.append(field.with_type(pa.uint64()))
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

    if compacting:
        assert (
            indexer and id_field and resolution is not None and parent_res is not None
        )
        dggs_col = f"{indexer.dggs}_{resolution:02}"
        df = table.to_pandas()
        df = indexer.compaction(
            df, resolution, list(df.columns), dggs_col, id_field, parent_res
        )
        geom_fn = _geom_fn(indexer, geo) if geo else None
        if geom_fn is not None:
            cells = df.index.to_series(index=df.index)
            df = df.assign(geometry=shapely.to_wkb(cells.map(geom_fn), hex=False))

        # Compaction and geometry reconstruction both need the working
        # (native, for capable backends) cell form; string output - the
        # final, one-time conversion - only happens here, after both.
        if (
            cell_id == const.CellIdMode.STRING.value
            and pa.string() != indexer.CELL_ARROW_TYPE
        ):
            df.index = pd.Index(indexer.cells_to_string(df.index), name=df.index.name)

        table = pa.Table.from_pandas(df, preserve_index=True)
        if geom_fn is not None:
            table = _with_geoparquet_metadata(table)

    merged = partition_dir / f"part.0-{uuid4().hex}.parquet"
    pq.write_table(table, merged, compression=compression)
    for f in files:
        f.unlink()


def _merge_output(
    indexer: VectorIndexer,
    output_dir: Path | str,
    resolution: int,
    parent_res: int,
    id_field: str | None,
    compact: bool,
    geo: str,
    compression: str,
    processes: int,
    cell_id: str,
) -> None:
    """
    Consolidate each hive partition directory to a single file (aggregating
    GeoParquet metadata), compacting per-directory when requested: the hive
    write routes a parent cell's every row into its directory, and the
    resolution floor keeps compaction within one parent, so no shuffle is
    needed and peak memory is one parent cell's data. Directories are
    processed in a process pool: compaction is pure Python, so threads
    would serialise on the GIL.
    """
    dirs = [d for d in sorted(Path(output_dir).iterdir()) if d.is_dir()]
    desc = (
        "Compacting and merging" if compact else "Merging to one file per parent cell"
    )
    with ProcessPoolExecutor(
        max_workers=max(1, processes), mp_context=_mp_context()
    ) as executor:
        futures = [
            executor.submit(
                _merge_partition_files,
                d,
                compression,
                indexer=indexer if compact else None,
                resolution=resolution if compact else None,
                parent_res=parent_res if compact else None,
                id_field=id_field if compact else None,
                geo=geo if compact else None,
                cell_id=cell_id,
            )
            for d in dirs
        ]
        for future in tqdm(as_completed(futures), total=len(futures), desc=desc):
            future.result()

    LOGGER.debug("Output writing complete")


def _polyfill(
    indexer: VectorIndexer,
    pq_in: Path,
    resolution: int,
    parent_res: int,
    output_directory: str,
    compression: str,
    id_col: str,
    geo: str,
    compact: bool,
    cell_id: str,
) -> np.ndarray:
    """
    Reads a geoparquet piece file, performs polyfilling (for Polygon),
    linetracing (for LineString), or indexing (for Point), and writes the
    cells hive-partitioned by parent cell directly into the output
    directory. Returns the ids of features that produced at least one cell.
    """
    # id_col is already a plain column by this point (_clean_geometries put
    # it there), so no reset_index() here: the staged file's own index is a
    # meaningless post-explode position, and materialising it would leak a
    # stray "index" column into every output row.
    df = gpd.read_parquet(pq_in)
    if df.empty:
        return np.array([])

    # DGGS specific conversion
    df = indexer.polyfill(df, resolution)

    if df.empty:
        # e.g. features smaller than a cell at this resolution
        return np.array([])

    df.index.rename(f"{indexer.dggs}_{resolution:02}", inplace=True)

    # Secondary (parent) index, used for hive partitioning
    df = indexer.secondary_index(df, parent_res)

    # Computed before write_partition, which may mutate/filter df in place
    indexed_ids = df[id_col].unique()

    # With compaction, geometry is serialised after compacting (merge step)
    geom_fn = None if compact else _geom_fn(indexer, geo)
    write_partition(
        df,
        geom_fn,
        Path(output_directory),
        f"{indexer.dggs}_{parent_res:02}",
        f"{indexer.dggs}_{resolution:02}",
        compression,
        indexer,
        cell_id,
        compact,
    )
    return indexed_ids


def _polyfill_star(args) -> np.ndarray:
    return _polyfill(*args)


def _metres_per_unit(crs: pyproj.CRS) -> float:
    axis = crs.axis_info[0]
    # unit_conversion_factor: linear units -> metres, angular -> radians
    return axis.unit_conversion_factor * (
        1 if crs.is_projected else const.EARTH_MEAN_RADIUS_M
    )


def _split_linestring_at_vertices(line, budget: float) -> list:
    """
    Split a LineString at existing vertices whenever cumulative arc length
    exceeds the budget. Vertex-only cuts leave every vertex-to-vertex
    segment untouched, so every backend traces exactly the segments it
    would have traced uncut; the shared vertex's cell appearing in both
    pieces is absorbed by the one-row-per-(feature, cell) contract.
    """
    coords = list(line.coords)
    pieces = []
    start, acc = 0, 0.0
    for i in range(1, len(coords) - 1):
        x0, y0 = coords[i - 1][:2]
        x1, y1 = coords[i][:2]
        acc += ((x1 - x0) ** 2 + (y1 - y0) ** 2) ** 0.5
        if acc >= budget:
            pieces.append(LineString(coords[start : i + 1]))
            start, acc = i, 0.0
    pieces.append(LineString(coords[start:]))
    return pieces


def _derive_cut_threshold(
    df: pd.DataFrame,
    dggs: str,
    resolution: int,
    cut_threshold: None | float = None,
) -> float:
    # cut_threshold == 0 disables bisection entirely
    if df.crs is None:
        raise ValueError(
            "Input has no CRS, which is required for indexing. "
            "Provide a dataset with a defined CRS."
        )

    if cut_threshold is None:
        metres_per_unit = _metres_per_unit(df.crs)
        cut_threshold_m2 = const.DEFAULT_AREA_THRESHOLD_M2(dggs, int(resolution))
        cut_threshold = cut_threshold_m2 / metres_per_unit**2
        LOGGER.debug(
            f"Using default cut_threshold of {cut_threshold} "
            f"({df.crs.axis_info[0].unit_name}^2)"
        )

    return float(cut_threshold)


def _blade_segment(
    indexer: VectorIndexer, dggs: str, resolution: int, cut_crs: pyproj.CRS
) -> float | None:
    """
    Max vertex spacing (in cut CRS units) along bisection cut edges. Needed
    whenever straight cut edges get reinterpreted as curves downstream —
    geodesic backends read them as great circles, and cutting in a projected
    CRS bends them on reprojection — because adjacent pieces carrying
    different vertices along the same cut line then diverge, losing cells
    whose centres fall between the two curves.
    """
    if not (indexer.GEODESIC_POLYFILL or cut_crs.is_projected):
        return None
    eps_m = max(const.DGGS_CELL_AREA_M2_BY_RES[dggs](resolution) ** 0.5 / 10, 10.0)
    return eps_m / _metres_per_unit(cut_crs)


def bisect_geometry(geometry, cut_threshold, blade_segment=None):
    cuts: list[tuple[bool, float]] = []
    pieces = katana.katana(geometry, cut_threshold, cuts=cuts)
    if blade_segment is not None:
        pieces = [katana.densify_cut_edges(g, cuts, blade_segment) for g in pieces]
    return GeometryCollection(pieces)


def _db_table(con: sqlalchemy.engine.Connection, layer: str) -> sqlalchemy.Table:
    parts = layer.rsplit(".", 1)
    schema, tbl_name = (parts[0], parts[1]) if len(parts) == 2 else (None, parts[0])
    return sqlalchemy.Table(
        tbl_name, sqlalchemy.MetaData(), schema=schema, autoload_with=con
    )


def _feature_count(
    input_file: Path | str, layer: str | None, con: SQLConnectionType | None
) -> int:
    if layer and con:
        with con.connect() as connection:
            tbl = _db_table(connection, layer)
            return connection.execute(
                sqlalchemy.select(sqlalchemy.func.count()).select_from(tbl)
            ).scalar_one()
    return pyogrio.read_info(str(input_file), layer=layer)["features"]


def _approx_frame_bytes(df: gpd.GeoDataFrame) -> int:
    """
    In-memory footprint estimate: column data, geometry coordinates, and a
    per-geometry object overhead (which dominates for small geometries).
    """
    coord_bytes = int(shapely.get_num_coordinates(df.geometry.values).sum()) * 16
    return int(df.memory_usage(deep=False).sum()) + coord_bytes + 150 * len(df)


def _next_batch_rows(batch: gpd.GeoDataFrame) -> int:
    bytes_per_row = max(1, _approx_frame_bytes(batch) // max(1, len(batch)))
    return int(
        min(
            const.INGEST_MAX_BATCH_ROWS,
            max(1, const.TARGET_BATCH_BYTES // bytes_per_row),
        )
    )


def _read_batches(
    input_file: Path | str,
    layer: str | None,
    con: SQLConnectionType | None,
    keep_attributes: bool,
    id_field: str | None,
    geom_col: str,
    total: int,
    keep_attribute: tuple[str, ...] = (),
):
    """
    Yield the input in GeoDataFrame batches, so peak memory is bounded by
    the batch size rather than the input size. Row counts adapt to the
    observed bytes-per-feature: a probe batch first, then batches sized to
    const.TARGET_BATCH_BYTES from each previous batch's footprint — so
    vertex-heavy features get proportionally smaller batches.

    Only the columns actually needed (id_field, geometry, and whichever of
    keep_attributes/keep_attribute apply) are read from the source. When
    id_field names the file's FID/primary-key slot (e.g. Kart working
    copies, which promote the dataset PK there) rather than a regular
    field, it's read via fid_as_index instead of columns=, since a FID
    slot never appears in either.
    """
    rows = max(1, const.INGEST_PROBE_ROWS)
    if layer and con:
        with con.connect() as connection:
            tbl = _db_table(connection, layer)
            if keep_attribute:
                cols = dict.fromkeys(
                    [geom_col, *([id_field] if id_field else []), *keep_attribute]
                )
                stmt = sqlalchemy.select(*(tbl.c[c] for c in cols))
            elif keep_attributes:
                stmt = tbl.select()
            elif id_field:
                stmt = sqlalchemy.select(tbl.c[id_field], tbl.c[geom_col])
            else:
                stmt = sqlalchemy.select(tbl.c[geom_col])
            # ctid ordering makes OFFSET/LIMIT a stable partition of the table
            stmt = stmt.order_by(sqlalchemy.text("ctid"))
            offset = 0
            while offset < total:
                result = gpd.read_postgis(
                    stmt.limit(rows).offset(offset), connection, geom_col=geom_col
                )
                if geom_col != "geometry":
                    result = result.rename_geometry("geometry")
                yield result
                offset += len(result)
                rows = _next_batch_rows(result)
        return
    id_is_fid = bool(id_field) and id_field == _fid_column(input_file, layer)
    if keep_attribute:
        columns = list(
            dict.fromkeys(
                [*keep_attribute, *([id_field] if id_field and not id_is_fid else [])]
            )
        )
    elif keep_attributes:
        columns = None
    else:
        columns = [id_field] if id_field and not id_is_fid else []
    offset = 0
    while offset < total:
        batch = gpd.read_file(
            input_file,
            layer=layer,
            skip_features=offset,
            max_features=rows,
            columns=columns,
            fid_as_index=id_is_fid,
        )
        if id_is_fid:
            batch = batch.rename_axis(id_field).reset_index()
        yield batch
        offset += len(batch)
        rows = _next_batch_rows(batch)


def _dictionary_encode_attributes(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Cast string attribute columns (object-dtype or pandas' newer StringDtype)
    to pandas' category dtype.

    Attribute values are attached once per input feature here, then
    duplicated onto every cell that feature explodes into by polyfill() --
    so a column's distinct-value count is bounded by feature count, while
    its row count downstream is bounded by (much larger) cell count.
    Dictionary encoding turns that duplication into small integer codes
    referencing one shared dictionary instead of independent string copies.
    This is preserved through the rest of the pipeline (explode, boolean
    subsetting, Arrow conversion) as long as frames built from genuinely
    different sources are combined via pyarrow rather than pandas -- pandas'
    own concat silently drops dictionary encoding when inputs don't share
    the same categories object.
    """
    geom_col = df.geometry.name
    string_cols = [
        c for c in df.columns if c != geom_col and pd.api.types.is_string_dtype(df[c])
    ]
    if string_cols:
        df = df.astype(dict.fromkeys(string_cols, "category"))
    return df


def _prepare_dataframe(
    df: gpd.GeoDataFrame,
    id_field: str | None,
    keep_attributes: bool,
    keep_attribute: tuple[str, ...] = (),
    fid_offset: int = 0,
) -> gpd.GeoDataFrame:
    if id_field:
        df = df.set_index(id_field)
    else:
        df = df.reset_index(drop=True)
        df.index = pd.RangeIndex(fid_offset, fid_offset + len(df), name="fid")
    if keep_attribute:
        df = df.loc[:, [c for c in keep_attribute if c in df.columns] + ["geometry"]]
        df = _dictionary_encode_attributes(df)
    elif keep_attributes:
        df = _dictionary_encode_attributes(df)
    else:
        df = df.loc[:, ["geometry"]]
    return df


def _run_bisection(
    df: gpd.GeoDataFrame,
    cut_threshold: None | float,
    processes: int,
    blade_segment: None | float = None,
    line_budget: None | float = None,
    pbar: tqdm | None = None,
) -> gpd.GeoDataFrame:
    """
    Geometry-type-aware bisection: polygonal features are cut by katana when
    their bbox area exceeds cut_threshold; linestrings are split at existing
    vertices when their arc length exceeds line_budget; points pass through.
    cut_threshold == 0 disables all bisection.

    pbar, if given, is a shared bar whose total grows by this call's
    oversized-feature count; otherwise a local bar is created and closed.
    """
    LOGGER.debug("Bisecting large geometries")
    if cut_threshold is not None and cut_threshold > 0:
        geom_type = df.geometry.geom_type
        geometry_loc = df.columns.get_loc("geometry")

        if line_budget is not None and line_budget > 0:
            line_mask = geom_type.isin(("LineString", "MultiLineString")).to_numpy()
            lengths = df.geometry.length.to_numpy()
            for pos in np.flatnonzero(line_mask & (lengths > line_budget)):
                g = df.geometry.iloc[pos]
                parts = g.geoms if g.geom_type == "MultiLineString" else [g]
                pieces = [
                    piece
                    for part in parts
                    for piece in _split_linestring_at_vertices(part, line_budget)
                ]
                df.iloc[pos, geometry_loc] = GeometryCollection(pieces)

        # katana's own early exit is exactly this bbox-area check; computing
        # it vectorized upfront (rather than dispatching every row to the
        # thread pool regardless) skips that overhead for features that
        # don't need bisecting at all.
        bounds = df.geometry.bounds
        bbox_area = (bounds["maxx"] - bounds["minx"]) * (
            bounds["maxy"] - bounds["miny"]
        )
        polygonal = geom_type.isin(
            ("Polygon", "MultiPolygon", "GeometryCollection")
        ).to_numpy()
        # Positions (not index labels) of oversized rows: the index is the
        # user-supplied id_field, which is not guaranteed to be unique, and
        # label-based assignment would write one feature's result into every
        # row sharing that label.
        oversized_positions = np.flatnonzero(
            polygonal & (bbox_area.to_numpy() > cut_threshold)
        )
        LOGGER.debug(
            "%d of %d features exceed the bisection area threshold",
            len(oversized_positions),
            len(df),
        )
        if len(oversized_positions):
            owns_pbar = pbar is None
            active_pbar: tqdm = (
                pbar
                if pbar is not None
                else tqdm(total=len(oversized_positions), desc="Bisection")
            )
            if not owns_pbar:
                active_pbar.total = (active_pbar.total or 0) + len(oversized_positions)
                active_pbar.refresh()
            try:
                with ThreadPoolExecutor(max_workers=max(1, processes)) as executor:
                    futures = [
                        (
                            pos,
                            executor.submit(
                                bisect_geometry,
                                df.geometry.iloc[pos],
                                cut_threshold,
                                blade_segment,
                            ),
                        )
                        for pos in oversized_positions
                    ]
                    for pos, future in futures:
                        df.iloc[pos, geometry_loc] = future.result()
                        active_pbar.update(1)
            finally:
                if owns_pbar:
                    active_pbar.close()
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


def _staged_file_chunks(
    batch: gpd.GeoDataFrame, dggs: str, resolution: int, max_rows: int
) -> Iterator[tuple[int, int]]:
    """
    Yield (start, end) row-position ranges bundling batch into successive
    staged files (and therefore _polyfill() worker tasks), so that no one
    file's estimated total cell output much exceeds
    const.MAX_CELLS_PER_STAGED_FILE, in addition to the max_rows backstop.

    Per-row cell counts are estimated from bounding-box area -- the same
    approximation bisection itself uses to size cut pieces -- converting
    degrees to an approximate metre scale when the CRS is geographic. This
    is only a heuristic bound: an exact count would mean running polyfill
    itself, which is the expensive step this is sizing work for.
    """
    bounds = batch.geometry.bounds
    bbox_area = (bounds["maxx"] - bounds["minx"]) * (bounds["maxy"] - bounds["miny"])
    if batch.crs is not None and batch.crs.is_geographic:
        axis = batch.crs.axis_info[0]
        metres_per_unit = axis.unit_conversion_factor * const.EARTH_MEAN_RADIUS_M
        bbox_area = bbox_area * metres_per_unit**2
    cell_area_m2 = const.DGGS_CELL_AREA_M2_BY_RES[dggs](resolution)
    est_cells = np.maximum(1.0, bbox_area.to_numpy() / cell_area_m2)

    start = 0
    running = 0.0
    for i, cells in enumerate(est_cells):
        if i > start and (
            running + cells > const.MAX_CELLS_PER_STAGED_FILE or i - start >= max_rows
        ):
            yield start, i
            start = i
            running = 0.0
        running += cells
    if start < len(est_cells):
        yield start, len(est_cells)


def _mp_context() -> multiprocessing.context.BaseContext:
    """Never fork: the parent is multi-threaded by the time the pool starts."""
    methods = multiprocessing.get_all_start_methods()
    return multiprocessing.get_context(
        "forkserver" if "forkserver" in methods else "spawn"
    )


def _run_dggs_indexing(
    indexer: VectorIndexer,
    filepaths: list,
    resolution: int,
    parent_res: int,
    output_dir: str,
    compression: str,
    processes: int,
    id_col: str,
    geo: str,
    compact: bool,
    cell_id: str,
) -> set:
    LOGGER.debug("DGGS indexing by spatial partitions with resolution: %d", resolution)
    args = [
        (
            indexer,
            filepath,
            resolution,
            parent_res,
            output_dir,
            compression,
            id_col,
            geo,
            compact,
            cell_id,
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
    processes: int,
    *,
    cut_threshold: None | float = None,
    compression: str = "snappy",
    id_field: str | None = None,
    con: SQLConnectionType | None = None,
    layer: str | None = None,
    geom_col: str = "geom",
    geo: str = const.GeoOutputMode.NONE.value,
    overwrite: bool = False,
    compact: bool = False,
    keep_attribute: tuple[str, ...] = (),
    cell_id: str = const.CellIdMode.STRING.value,
) -> Path | str:
    """
    Performs multi-threaded DGGS indexing on geometries (including multipart and collections).

    The run is staged in a sibling directory and only moved into place on
    success, so a failed run never destroys previous output (with overwrite)
    nor leaves a half-written target behind.
    """
    layer = resolve_layer(input_file, layer, con)
    output_directory = resolve_output_path(output_directory, overwrite)
    staging = output_directory.parent / f".{output_directory.name}.staging"
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)
    try:
        _index(
            dggs,
            input_file,
            staging,
            resolution,
            parent_res,
            keep_attributes,
            cut_threshold,
            processes,
            compression,
            id_field,
            con,
            layer,
            geom_col,
            geo,
            compact,
            keep_attribute,
            cell_id,
        )
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return _commit_output(staging, output_directory, overwrite)


def _index(
    dggs: str,
    input_file: Path | str,
    output_directory: Path,
    resolution: int,
    parent_res: None | str | int,
    keep_attributes: bool,
    cut_threshold: None | float,
    processes: int,
    compression: str,
    id_field: str | None,
    con: SQLConnectionType | None,
    layer: str | None,
    geom_col: str,
    geo: str,
    compact: bool,
    keep_attribute: tuple[str, ...] = (),
    cell_id: str = const.CellIdMode.STRING.value,
) -> None:
    id_field = id_field or resolve_default_id_field(input_file, layer, con)
    indexer = idxfactory.indexer_instance(dggs)
    check_cell_id(cell_id, indexer)
    parent_res = get_parent_res(dggs, parent_res, resolution)

    total = _feature_count(input_file, layer, con)
    if total == 0:
        LOGGER.warning(
            "Input contained 0 features (layer=%s). Nothing to index; exiting.",
            layer if layer else "<default>",
        )
        return

    # a handful of staged files per worker balances the pool; row count is
    # a backstop cap, but _staged_file_chunks additionally bounds files by
    # estimated cell output, since row count alone isn't a safe proxy for
    # worker memory (see const.MAX_CELLS_PER_STAGED_FILE)
    rows_per_file = min(
        const.STAGED_FILE_MAX_ROWS,
        max(
            1,
            math.ceil(total / (const.STAGED_FILES_PER_WORKER * max(1, processes))),
        ),
    )

    features_in: set = set()
    blade_segment: float | None = None
    line_budget: float | None = None

    with tempfile.TemporaryDirectory(suffix=".parquet") as tmpdir:
        fid_offset = 0
        part = 0
        pbar = tqdm(total=total, desc="Ingesting", unit="feature")
        bisection_pbar = tqdm(total=0, desc="Bisection", unit="feature")
        for batch in _read_batches(
            input_file,
            layer,
            con,
            keep_attributes,
            id_field,
            geom_col,
            total,
            keep_attribute,
        ):
            pbar.update(len(batch))
            cut_threshold = _derive_cut_threshold(
                batch, dggs, resolution, cut_threshold
            )
            batch = _prepare_dataframe(
                batch, id_field, keep_attributes, keep_attribute, fid_offset=fid_offset
            )
            fid_offset += len(batch)
            features_in.update(batch.index)
            if line_budget is None:
                blade_segment = _blade_segment(indexer, dggs, resolution, batch.crs)
                # linestrings cross roughly one cell per edge-length travelled
                line_budget = (
                    const.DEFAULT_CUT_CELLS_PER_PIECE
                    * const.DGGS_CELL_AREA_M2_BY_RES[dggs](resolution) ** 0.5
                    / _metres_per_unit(batch.crs)
                )
            batch = _run_bisection(
                batch,
                cut_threshold,
                processes,
                blade_segment,
                line_budget=line_budget,
                pbar=bisection_pbar,
            )
            batch = _clean_geometries(batch, indexer)
            for start, end in _staged_file_chunks(
                batch, dggs, resolution, rows_per_file
            ):
                batch.iloc[start:end].to_parquet(
                    PurePath(tmpdir, f"part-{part:06}.parquet")
                )
                part += 1
        pbar.close()
        if bisection_pbar.n == 0:  # nothing bisected: don't leave an empty bar
            bisection_pbar.leave = False
        bisection_pbar.close()
        filepaths = [f.absolute() for f in Path(tmpdir).glob("*")]

        indexed_ids = _run_dggs_indexing(
            indexer,
            filepaths,
            resolution,
            parent_res,
            str(output_directory),
            compression,
            processes,
            id_field or "fid",
            geo,
            compact,
            cell_id,
        )
        dropped = features_in - indexed_ids
        if dropped:
            LOGGER.warning(
                "%d of %d features produced no cells at resolution %s and were omitted",
                len(dropped),
                len(features_in),
                resolution,
            )
        if not any(d.is_dir() for d in Path(output_directory).iterdir()):
            LOGGER.warning(
                "No features were indexed (resolution %s may be too coarse for the input). Nothing to write; exiting.",
                resolution,
            )
            return

        _merge_output(
            indexer,
            output_directory,
            resolution,
            parent_res,
            id_field or "fid",
            compact,
            geo,
            compression,
            processes,
            cell_id,
        )
