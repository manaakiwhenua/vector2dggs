# vector2dggs

[![pypi](https://img.shields.io/pypi/v/vector2dggs?label=vector2dggs)](https://pypi.org/project/vector2dggs/)

Python-based CLI tool to index vector files to DGGS in parallel, writing out to Parquet.

This is the vector equivalent of [raster2dggs](https://github.com/manaakiwhenua/raster2dggs).

Currently this tool supports the following DGGSs:

- [H3](https://h3geo.org/)
- [rHEALPix](https://datastore.landcareresearch.co.nz/dataset/rhealpix-discrete-global-grid-system)
- [S2](https://s2geometry.io/)
- [A5](https://a5geo.org/)

... and the following geocode systems:

- [Geohash](https://en.wikipedia.org/wiki/Geohash)

Contributions (especially for other DGGSs), suggestions, bug reports and strongly worded letters are all welcome.

![Land cover polygons indexed to H3 resolutions 10 and 12, with the hive-partitioned Parquet output they produce](./docs/imgs/indexing-example.png "Land cover polygons indexed to H3 resolutions 10 and 12, with the hive-partitioned Parquet output they produce")

## Installation

This tool makes use of optional extras to allow you to install a limited subset of DGGSs.

If you want all possible:

```bash
pip install vector2dggs[all]
```

If you want only a subset, use the pattern `pip install vector2dggs[rhp]` (for one) or `pip install vector2dggs[h3,s2]` (for multiple).

A bare `pip install vector2dggs` **will not install any DGGS backends**.

PostgreSQL/PostGIS input requires the `postgres` extra (e.g. `pip install vector2dggs[h3,postgres]`); it is included in `all`.


## Usage

All commands (`h3`, `rhp`, `s2`, `a5`, `geohash`) share the same interface:

```bash
vector2dggs <dggs> [OPTIONS] VECTOR_INPUT OUTPUT_DIRECTORY
```

`VECTOR_INPUT` may be a local file (anything GDAL can read), a remote URI or GDAL virtual path (e.g. `https://…`, `/vsizip/…`), or a PostgreSQL/PostGIS connection URL (with `-lyr` naming the table). `OUTPUT_DIRECTORY` is written as an Apache Parquet data store: a directory with one file per partition.

- `-r`/`--resolution`: the target DGGS resolution. Each output row is one (feature, cell) pair; by default a cell is included when its centre point falls inside the feature, uniformly across all backends (see `-m`/`--mode`).
- `-pr`/`--parent_res`: a coarser resolution used to partition the output (hive directories such as `h3_03=…`); defaults to a fixed offset below the target resolution.
- `-id`/`--id_field`: the feature identifier carried into the output. Defaults to the input's own internal ID where one exists (a GPKG's FID column, or a DB table's single-column primary key); otherwise falls back to a synthetic index tied to row position in the read order — stable across repeated runs of the same unchanged input, but not portable to a different export/copy of the same data. Rows sharing an id are treated as one feature.
- `-co`/`--compact`: merges complete sets of sibling cells belonging to one feature (grouped by whichever id_field is in play, explicit, auto-detected, or synthetic), to no coarser than the parent resolution. Compacted output expands back to exactly the full-resolution result.
- `--geo`: plain Parquet by default; `point` or `polygon` writes GeoParquet (v1.1.0) cell geometries instead.
- `-m`/`--mode`: which of a polygon's cells are indexed. See [Containment modes](#containment-modes) below.
- `--cell-id`: `string` (default) or `uint64`. DGGS with a native integer cell form (A5, H3, S2) can write cell IDs as unsigned 64-bit integers instead of text — useful where downstream tools take integer cell IDs directly (e.g. DuckDB's `h3` extension). Cell IDs are worked in the native form internally regardless of this flag; it only controls the final output rendering. String-only DGGS (rHEALPix, Geohash) reject `--cell-id uint64`.

If nothing is indexed — usually because the resolution is too coarse for the input — the run warns and writes an empty dataset: one zero-row file carrying the schema the run would have produced, so a reader sees an empty layer rather than an unreadable directory.

The full reference (`vector2dggs h3 --help`; the other commands differ only in their resolution ranges):

```
Usage: vector2dggs h3 [OPTIONS] VECTOR_INPUT OUTPUT_DIRECTORY

  Ingest a vector dataset and index it to the H3 DGGS.

  VECTOR_INPUT is the path to input vector geospatial data. OUTPUT_DIRECTORY
  should be a directory, not a file or database table, as it will instead be the
  write location for an Apache Parquet data store.

Options:
  -v, --verbosity LVL             Either CRITICAL, ERROR, WARNING, INFO or DEBUG
                                  [default: INFO]
  -r, --resolution [0|1|2|3|4|5|6|7|8|9|10|11|12|13|14|15]
                                  H3 resolution to index  [required]
  -pr, --parent_res [0|1|2|3|4|5|6|7|8|9|10|11|12|13|14|15]
                                  H3 parent resolution for the output partition.
                                  Defaults to resolution - 6
  -id, --id_field TEXT            Field to use as an ID; defaults to the input's
                                  own internal ID if it has one (e.g. a GPKG's
                                  FID column, or a DB table's single-column
                                  primary key), otherwise falls back to a
                                  constructed 0...n index on the original
                                  feature order.
  -k, --keep_attributes           Retain attributes in output. The default is to
                                  create an output that only includes H3 cell ID
                                  and the ID given by the -id field (or the
                                  default index ID).
  -ka, --keep_attribute TEXT      Retain only this attribute in output; repeat
                                  for multiple. Takes precedence over
                                  -k/--keep_attributes.
  -p, -t, --processes, --threads INTEGER RANGE
                                  Number of parallel workers: process pools for
                                  indexing and for merging/compaction, and the
                                  thread pool for bisection.  [default: (CPU
                                  count - 1, capped by available memory); x>=1]
  -cp, --compression TEXT         Compression method to use for the output
                                  Parquet files. Options include 'snappy',
                                  'gzip', 'brotli', 'lz4', 'zstd', etc. Use
                                  'none' for no compression.  [default: snappy]
  -lyr, --layer TEXT              Name of the layer or table to read when using
                                  an input that supports layers or tables
  -g, --geom_col TEXT             Column name to use when using a spatial
                                  database connection as input  [default: geom]
  --geo [none|point|polygon]      Select geometry encoding for the output:
                                  'none' for regular Parquet (no GeoParquet
                                  metadata), or 'point'/'polygon' to write
                                  GeoParquet (v1.1.0) with the corresponding
                                  geometry type.  [default: none]
  --cell-id [string|uint64]       Cell ID output form: 'string' (default) or
                                  'uint64' (unsigned 64-bit integer; e.g. for
                                  DuckDB interop).  [default: string]
  -m, --mode [centre|intersects|within]
                                  How a polygon's cells are chosen, named for
                                  the DE-9IM predicate each applies: 'centre'
                                  takes each cell whose centre point falls
                                  inside the feature; 'intersects' takes every
                                  cell whose area meets the feature, covering it
                                  completely, so a feature smaller than a cell
                                  still produces one (rasterio's all_touched);
                                  'within' takes only cells whose area lies
                                  wholly inside it. Linestrings and points are
                                  indexed the same way in every mode.  [default:
                                  centre]
  --tempdir PATH                  Temporary data is created during the execution
                                  of this program. This parameter allows you to
                                  control where this data will be written.
                                  [default: (system temp dir)]
  -co, --compact                  Compact the H3 cells up to the parent
                                  resolution, grouping by id_field (explicit,
                                  auto-detected, or the default 0...n sequence).
  -o, --overwrite
  --version                       Show the version and exit.
  --help                          Show this message and exit.
```

vector2dggs is a command-line tool; the underlying Python API (`vector2dggs.common.index`) can be called directly but is not a stable, supported interface.

## Containment modes

Filling a polygon means deciding, cell by cell, whether the cell belongs to the feature — and that needs a rule for what counts. `-m`/`--mode` chooses it, and each mode is named for the [DE-9IM](https://en.wikipedia.org/wiki/DE-9IM) predicate it applies between the cell and the feature:

| Mode | A cell is indexed when… | Use it for |
| --- | --- | --- |
| `centre` (default) | its **centre point** falls inside the feature | one cell per place: cells partition the plane, so a coverage input gives a coverage output, with no cell claimed by two neighbouring features |
| `intersects` | its **area** meets the feature anywhere | complete coverage: querying by any cell that intersects a feature finds it, and a feature smaller than a cell still gets one (in `centre` mode such features produce nothing and are reported as omitted) |
| `within` | its **area** lies wholly inside the feature | conservative selection: every cell returned is unambiguously the feature's, at the cost of dropping the boundary |

The modes are nested: `within` ⊆ `centre` ⊆ `intersects`. Only polygons are affected — a traced line already covers every cell it meets, and a point has exactly one cell.

![The three containment modes compared across H3, S2, A5, rHEALPix and Geohash on the same land cover polygons](./docs/imgs/containment-modes.gif "The three containment modes compared across H3, S2, A5, rHEALPix and Geohash on the same land cover polygons")

Each backend applies the same rule to its own cells, so the three modes differ in the same way everywhere: `centre` tiles the plane, `intersects` spills past the feature's edge and gives a shared edge's cells to both neighbours, and `within` opens a gap along every boundary. (The same animation is also available as [an MP4](./docs/imgs/containment-modes.mp4), which GitHub will not play inline but which is crisper for slides.)

`within` is computed as the intersecting cells minus the cells the feature's own boundary passes through, rather than asked of the backend directly. The two are equivalent for a whole feature, but only the first survives bisection: vector2dggs cuts large polygons into pieces to bound memory, and a cell wholly inside a feature need not be wholly inside any single piece the cut lines cross. Both terms of the subtraction are taken before cutting, so neither depends on where the cuts fall.

`within` is unavailable for A5, whose library (`pya5`) offers no wholly-within test; `vector2dggs a5 -m within` is rejected rather than silently indexed some other way.

If you have arrived from raster tooling, `intersects` is what rasterio's `all_touched=True` and GDAL's `ALL_TOUCHED` do. It is deliberately **not** called `touched`, because DE-9IM already has a `touches` predicate meaning something close to the opposite — geometries meeting only at their boundaries, with their interiors disjoint. The backends' own vocabularies map straight across: H3's `center`/`overlap`/`full` and rHEALPix's `center`/`overlapping`/`full` are `centre`/`intersects`/`within`.

### What "cell" means here

A DGGS cell has no canonical rendering as either a point or an area, so each mode above is explicit about which it tests, and each backend answers with its own:

- **Centre point.** rHEALPix uses the cell's nucleus; geohash the midpoint of its box; H3, S2 and A5 their own cell centres.
- **Area.** A geohash cell is a longitude/latitude aligned box, bounded by parallels and meridians, so it is exactly what it appears to be; H3's cells are decided in longitude/latitude too; S2 and A5 work on a sphere, where cell edges are great-circle arcs; a rHEALPix cell is an exact quadrilateral in its own projection, which on the ellipsoid stays longitude/latitude aligned for equatorial cells but gives polar cells curved edges that are sampled.

So `intersects` and `within` are exact with respect to each backend's own model of a cell's extent, and near a feature's boundary different backends will not necessarily agree about the same cell — which is a property of the grids, not a defect in any one of them. The feature's own edges are read under the same model: as straight lines in longitude/latitude by H3, geohash and rHEALPix, and as great-circle arcs by S2 and A5. For short edges the two readings barely differ, but they are not interchangeable at length: a 40° edge at 60°N passes 170 km from where the great circle between its endpoints runs.

## Visualising output

Output is in the Apache Parquet format, a directory with one file per partition. With `--geo point` or `--geo polygon` output will be written as GeoParquet (v1.1.0) with the respective geometry types. GeoParquet can be visualised using desktop GIS tools.

The Apache Parquet output is indexed by an ID column (which you can specify), so it should be ready for two intended use-cases:
- Joining attribute data from the original feature-level data onto computed DGGS cells.
- Joining other data to this output on the DGGS cell ID. (The output has a column like `{dggs}_\d`, e.g. `h3_09` or `h3_12` according to the target resolution, zero-padded to account for the maximum resolution of the DGGS).

## Compaction

Compaction is supported with the `-co/--compact` argument. A complete set of sibling cells is replaced by their parent, as far up as the partition's parent resolution, so a feature's interior coarsens while its boundary stays at the target resolution. The coverage is unchanged; only the row count falls.

![The same land cover indexed to rHEALPix resolution 13, before and after compaction](docs/imgs/compaction-example.png "The same land cover indexed to rHEALPix resolution 13, before and after compaction")

Compaction respects overlapping polygons by considering each feature independently. This does mean that the index of the result is not necessarily unique (unless your input is a vector _coverage_, i.e. it does not have overlaps — as the example above is, so each of its cells belongs to exactly one feature). Where features do overlap, a cell covered by two of them appears once per feature.

### For development

In brief, to get started:

- Install [Poetry](https://python-poetry.org/docs/basic-usage/)
- Create and populate the virtual environment with `poetry install`. This will install necessary dependencies.

No system GDAL is required: vector data is read via [pyogrio](https://pyogrio.readthedocs.io/), whose wheels bundle GDAL. If you need a GDAL driver that pyogrio's bundled build lacks, build pyogrio from source against your own GDAL.
- Subsequently, activate the virtual environment with `eval "$(poetry env activate)"`.

If you run `poetry install -E all --with dev` and activate the environment with `eval "$(poetry env activate)"`, the CLI tool will be aliased so you can simply use `vector2dggs` rather than `poetry run vector2dggs`.

For partial backend support you can consider `poetry install --with dev -E h3 -E s2` etc. To check what is installed: `poetry show --tree`.

Alternatively, it is also possible to install using pip with `pip install -e .`, and bypass Poetry.

#### Code formatting, linting and type checking

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

Please run `black .`, `ruff check .` and `mypy vector2dggs/` before committing. All three are enforced in CI.

#### Tests

Tests are included. Some tests (covering the PostgreSQL/PostGIS input path) spin up a throwaway PostGIS container via Docker; they'll be skipped automatically if Docker isn't available, rather than failing the run.

To run them, activate the Poetry environment first (`eval "$(poetry env activate)"`), then run:

```bash
python -m pytest -n auto tests/
```

Or without activating the shell:

```bash
poetry run pytest -n auto tests/
```

To test a specific DGGS:

```bash
python -m pytest tests/test_runthrough.py -k "a5" -v
```

Test data are included at `tests/data/`.

## Example commands

With a local GPKG:

```bash
vector2dggs h3 -v DEBUG -id title_no -r 12 -o ~/Downloads/nz-property-titles.gpkg ~/Downloads/nz-property-titles.parquet

```

Indexing small polygons without losing any of them (see [Containment modes](#containment-modes)):

```bash
vector2dggs s2 -id parcel_id -r 16 -m intersects ./parcels.gpkg ./parcels.parquet
```

With a PostgreSQL/PostGIS connection:

```bash
vector2dggs h3 -v DEBUG -id ogc_fid -r 9 -pr 5 -t 4 --overwrite -lyr topo50_lake postgresql://user:password@host:port/db ./topo50_lake.parquet
```

## Citation

Citation metadata is maintained in [`CITATION.cff`](CITATION.cff). GitHub renders this as a **"Cite this repository"** button on the repository page (top-right of the About panel), which provides ready-to-copy BibTeX and APA formats.

[![manaakiwhenua-standards](https://github.com/manaakiwhenua/vector2dggs/workflows/manaakiwhenua-standards/badge.svg)](https://github.com/manaakiwhenua/manaakiwhenua-standards)
