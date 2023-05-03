# vector2dggs

[![pypi](https://img.shields.io/pypi/v/vector2dggs?label=vector2dggs)](https://pypi.org/project/vector2dggs/)

Python-based CLI tool to index raster files to DGGS in parallel, writing out to Parquet.

This is the vector equivalent of [raster2dggs](https://github.com/manaakiwhenua/raster2dggs).

Currently only supports H3 DGGS, and probably has other limitations since it has been developed for a specific internal use case, though it is intended as a general-purpose abstraction. Contributions, suggestions, bug reports and strongly worded letters are all welcome.

Currently only supports polygons; but both coverages (strictly non-overlapping polygons), and sets of polygons that do/may overlap, are supported. Overlapping polygons are captured by ensuring that DGGS cell IDs may be non-unique (repeated) in the output.

![Example use case for vector2dggs, showing parcels indexed to a high H3 resolution](./docs/imgs/vector2dggs-example.png "Example use case for vector2dggs, showing parcels indexed to a high H3 resolution")

## Installation

```bash
pip install vector2dggs
```

## Usage

```bash
vector2dggs h3 --help
Usage: vector2dggs h3 [OPTIONS] VECTOR_INPUT OUTPUT_DIRECTORY

  Ingest a vector dataset and index it to the H3 DGGS.

  VECTOR_INPUT is the path to input vector geospatial data. OUTPUT_DIRECTORY
  should be a directory, not a file, as it will be the write location for an
  Apache Parquet data store.

Options:
  -v, --verbosity LVL             Either CRITICAL, ERROR, WARNING, INFO or
                                  DEBUG  [default: INFO]
  -r, --resolution [0|1|2|3|4|5|6|7|8|9|10|11|12|13|14|15]
                                  H3 resolution to index  [required]
  -id, --id_field TEXT            Field to use as an ID; defaults to a
                                  constructed single 0...n index on the
                                  original feature order.
  -k, --keep_attributes           Retain attributes in output. The default is
                                  to create an output that only includes H3
                                  cell ID and the ID given by the -id field
                                  (or the default index ID).
  -p, --partitions INTEGER        The number of partitions to create.
                                  Recommendation: at least as many partitions
                                  as there are available `--threads`.
                                  Partitions are processed in parallel once
                                  they have been formed.  [default: 50;
                                  required]
  -s, --spatial_sorting [hilbert|morton|geohash]
                                  Spatial sorting method when perfoming
                                  spatial partitioning.  [default: hilbert]
  -crs, --cut_crs INTEGER         Set the coordinate reference system (CRS)
                                  used for cutting large polygons (see `--cur-
                                  threshold`). Defaults to the same CRS as the
                                  input. Should be a valid EPSG code.
  -c, --cut_threshold INTEGER     Cutting up large polygons into smaller
                                  pieces based on a target length. Units are
                                  assumed to match the input CRS units unless
                                  the `--cut_crs` is also given, in which case
                                  units match the units of the supplied CRS.
                                  [default: 5000; required]
  -t, --threads INTEGER           Amount of threads used for operation
                                  [default: 7]
  -tbl, --table TEXT              Name of the table to read when using a
                                  spatial database connection as input
  -g, --geom_col TEXT             Column name to use when using a spatial
                                  database connection as input  [default:
                                  geom]
  -o, --overwrite
  --help                          Show this message and exit.

```

### Example 




## Visualising output

Output is in the Apache Parquet format, a directory with one file per partition.

For a quick view of your output, you can read Apache Parquet with pandas, and then use h3-pandas and geopandas to convert this into a GeoPackage or GeoParquet for visualisation in a desktop GIS, such as QGIS. The Apache Parquet output is indexed by an ID column (which you can specify), so it should be ready for two intended use-cases:
- Joining attribute data from the original feature-level data onto computer DGGS cells.
- Joining other data to this output on the H3 cell ID. (The output has a column like `h3_\d{2}`, e.g. `h3_09` or `h3_12` according to the target resolution.)

Geoparquet output (hexagon boundaries):

```python
>>> import pandas as pd
>>> import h3pandas
>>> g = pd.read_parquet('./output-data/nz-property-titles.12.parquet').h3.h3_to_geo_boundary()
>>> g
                  title_no                                           geometry
h3_12                                                                        
8cbb53a734553ff  NA94D/635  POLYGON ((174.28483 -35.69315, 174.28482 -35.6...
8cbb53a734467ff  NA94D/635  POLYGON ((174.28454 -35.69333, 174.28453 -35.6...
8cbb53a734445ff  NA94D/635  POLYGON ((174.28416 -35.69368, 174.28415 -35.6...
8cbb53a734551ff  NA94D/635  POLYGON ((174.28496 -35.69329, 174.28494 -35.6...
8cbb53a734463ff  NA94D/635  POLYGON ((174.28433 -35.69335, 174.28432 -35.6...
...                    ...                                                ...
8cbb53a548b2dff  NA62D/324  POLYGON ((174.30249 -35.69369, 174.30248 -35.6...
8cbb53a548b61ff  NA62D/324  POLYGON ((174.30232 -35.69402, 174.30231 -35.6...
8cbb53a548b11ff  NA57C/785  POLYGON ((174.30140 -35.69348, 174.30139 -35.6...
8cbb53a548b15ff  NA57C/785  POLYGON ((174.30161 -35.69346, 174.30160 -35.6...
8cbb53a548b17ff  NA57C/785  POLYGON ((174.30149 -35.69332, 174.30147 -35.6...

[52736 rows x 2 columns]
>>> g.to_parquet('./output-data/parcels.12.geo.parquet')
```

### For development

In brief, to get started:

- Install [Poetry](https://python-poetry.org/docs/basic-usage/)
- Install [GDAL](https://gdal.org/)
    - If you're on Windows, `pip install gdal` may be necessary before running the subsequent commands.
    - On Linux, install GDAL 3.6+ according to your platform-specific instructions, including development headers, i.e. `libgdal-dev`.
- Create the virtual environment with `poetry init`. This will install necessary dependencies.
- Subsequently, the virtual environment can be re-activated with `poetry shell`.

If you run `poetry install`, the CLI tool will be aliased so you can simply use `vector2dggs` rather than `poetry run vector2dggs`, which is the alternative if you do not `poetry install`.

#### Code formatting

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

Please run `black .` before committing.

## Example commands

With a local GPKG:

```bash
vector2dggs h3 -v DEBUG -id title_no -r 12 -o ~/Downloads/nz-property-titles.gpkg ~/Downloads/nz-property-titles.parquet

```

With a PostgreSQL/PostGIS connection:

```bash
vector2dggs h3 -v DEBUG -id ogc_fid -r 9 -p 5 -t 4 --overwrite -tbl topo50_lake postgresql://user:password@host:port/db ./topo50_lake.parquet
```

## Citation

```bibtex
@software{vector2dggs,
  title={{vector2dggs}},
  author={Ardo, James and Law, Richard},
  url={https://github.com/manaakiwhenua/vector2dggs},
  version={0.3.1},
  date={2023-04-20}
}
```

APA/Harvard

> Ardo, J., & Law, R. (2023). vector2dggs (0.3.1) [Computer software]. https://github.com/manaakiwhenua/vector2dggs

[![manaakiwhenua-standards](https://github.com/manaakiwhenua/vector2dggs/workflows/manaakiwhenua-standards/badge.svg)](https://github.com/manaakiwhenua/manaakiwhenua-standards)
