# vector2dggs

[![pypi](https://img.shields.io/pypi/v/vector2dggs?label=vector2dggs)](https://pypi.org/project/vector2dggs/)

Python-based CLI tool to index vector files to DGGS in parallel, writing out to Parquet.

This is the vector equivalent of [raster2dggs](https://github.com/manaakiwhenua/raster2dggs).

Currently this tool supports the following DGGSs:

- [H3](https://h3geo.org/)
- [rHEALPix](https://datastore.landcareresearch.co.nz/dataset/rhealpix-discrete-global-grid-system)
- [S2](https://s2geometry.io/)

... and the following geocode systems:

- [Geohash](https://en.wikipedia.org/wiki/Geohash) (points, polygons)

Contributions (especially for other DGGSs), suggestions, bug reports and strongly worded letters are all welcome.

![Example use case for vector2dggs, showing parcels indexed to a high H3 resolution](./docs/imgs/vector2dggs-example.png "Example use case for vector2dggs, showing parcels indexed to a high H3 resolution")

## Installation

```bash
pip install vector2dggs
```

## Usage

```bash
vector2dggs --help

Usage: vector2dggs [OPTIONS] COMMAND [ARGS]...

Options:
  --version  Show the version and exit.
  --help     Show this message and exit.

Commands:
  geohash  Ingest a vector dataset and index it using the Geohash geocode...
  h3       Ingest a vector dataset and index it to the H3 DGGS.
  rhp      Ingest a vector dataset and index it to the rHEALPix DGGS.
  s2       Ingest a vector dataset and index it to the S2 DGGS.
```

```bash
vector2dggs h3 --help
Usage: vector2dggs h3 [OPTIONS] VECTOR_INPUT OUTPUT_DIRECTORY

  Ingest a vector dataset and index it to the H3 DGGS.

  VECTOR_INPUT is the path to input vector geospatial data. OUTPUT_DIRECTORY
  should be a directory, not a file or database table, as it will instead be
  the write location for an Apache Parquet data store.

Options:
  -v, --verbosity LVL             Either CRITICAL, ERROR, WARNING, INFO or
                                  DEBUG  [default: INFO]
  -r, --resolution [0|1|2|3|4|5|6|7|8|9|10|11|12|13|14|15]
                                  H3 resolution to index  [required]
  -pr, --parent_res [0|1|2|3|4|5|6|7|8|9|10|11|12|13|14|15]
                                  H3 Parent resolution for the output
                                  partition. Defaults to resolution - 6
  -id, --id_field TEXT            Field to use as an ID; defaults to a
                                  constructed single 0...n index on the
                                  original feature order.
  -k, --keep_attributes           Retain attributes in output. The default is
                                  to create an output that only includes H3
                                  cell ID and the ID given by the -id field
                                  (or the default index ID).
  -ch, --chunksize INTEGER        The number of rows per index partition to
                                  use when spatially partioning. Adjusting
                                  this number will trade off memory use and
                                  time.  [default: 50; required]
  -s, --spatial_sorting [hilbert|morton|geohash|none]
                                  Spatial sorting method when perfoming
                                  spatial partitioning.  [default: none]
  -crs, --cut_crs INTEGER         Set the coordinate reference system (CRS)
                                  used for cutting large geometries (see
                                  `--cut_threshold`). Defaults to the same CRS
                                  as the input. Should be a valid EPSG code.
  -c, --cut_threshold INTEGER     Cutting up large geometries into smaller
                                  geometries based on a target length. Units
                                  are assumed to match the input CRS units
                                  unless the `--cut_crs` is also given, in
                                  which case units match the units of the
                                  supplied CRS.  [default: 5000; required]
  -t, --threads INTEGER           Amount of threads used for operation
                                  [default: NUM_CPUS - 1]
  -cp, --compression TEXT         Compression method to use for the output
                                  Parquet files. Options include 'snappy',
                                  'gzip', 'brotli', 'lz4', 'zstd', etc. Use
                                  'none' for no compression.  [default:
                                  snappy]
  -lyr, --layer TEXT              Name of the layer or table to read when
                                  using an input that supports layers or
                                  tables
  -g, --geom_col TEXT             Column name to use when using a spatial
                                  database connection as input  [default:
                                  geom]
  --tempdir PATH                  Temporary data is created during the
                                  execution of this program. This parameter
                                  allows you to control where this data will
                                  be written.
  -co, --compact                  Compact the H3 cells up to the parent
                                  resolution. Compaction requires an id_field.
  -o, --overwrite
  --version                       Show the version and exit.
  --help                          Show this message and exit.
```

## Visualising output

Output is in the Apache Parquet format, a directory with one file per partition.

For a quick view of your output, you can read Apache Parquet with pandas, and then use tools like h3-pandas and geopandas to convert this into a GeoPackage or GeoParquet for visualisation in a desktop GIS, such as QGIS. The Apache Parquet output is indexed by an ID column (which you can specify), so it should be ready for two intended use-cases:
- Joining attribute data from the original feature-level data onto computer DGGS cells.
- Joining other data to this output on the DGGS cell ID. (The output has a column like `{dggs}_\d`, e.g. `h3_09` or `h3_12` according to the target resolution, zero-padded to account for the maximum resolution of the DGGS)

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

An example for S2 output (using `s2sphere`):


```python
import pandas as pd
import geopandas as gpd
import s2sphere
from shapely.geometry import Polygon

RES = 18
df = pd.read_parquet(f'~/output-data/ponds-with-holes.s2.{RES}.pq')

def s2id_to_polygon(s2_id_hex):
    cell_id = s2sphere.CellId.from_token(s2_id_hex)
    cell = s2sphere.Cell(cell_id)
    vertices = []
    for i in range(4):
        vertex = cell.get_vertex(i)
        lat_lng = s2sphere.LatLng.from_point(vertex)
        vertices.append((lat_lng.lng().degrees, lat_lng.lat().degrees))  # (lon, lat)
    return Polygon(vertices)

df['geometry'] = df.index.to_series().apply(s2id_to_polygon)
df = gpd.GeoDataFrame(df, geometry='geometry', crs='EPSG:4326')  # WGS84
df.to_parquet(f'sample-{RES}.parquet')
```

## Compaction

Compaction is supported with the `-co/--compact` argument. The result respects overlapping polygons by considering each feature independently. (In the below example output for rHEALPix, cells are shown with opacity; overlap is visible where there is a darker shade.) This does mean that the index of the result is not necessarily unique (unless your input is a vector _coverage_, i.e. it does not have overlaps.)

![Example of compaction of overlapping vector features with the rHEALPix DGGS](docs/imgs/rhp-compaction-example.png)

### For development

In brief, to get started:

- Install [Poetry](https://python-poetry.org/docs/basic-usage/)
- Install [GDAL](https://gdal.org/)
    - If you're on Windows, `pip install gdal` may be necessary before running the subsequent commands.
    - On Linux, install GDAL 3.8+ according to your platform-specific instructions, including development headers, i.e. `libgdal-dev`.
- Create the virtual environment with `poetry init`. This will install necessary dependencies.
  - If the installation of `s2geometry` fails, you may require SWIG to build it. (A command like `conda install swig` or `sudo dnf install swig` depending on your platform).
- Subsequently, the virtual environment can be re-activated with `poetry shell`.

If you run `poetry install`, the CLI tool will be aliased so you can simply use `vector2dggs` rather than `poetry run vector2dggs`, which is the alternative if you do not `poetry install`.

Alternatively, it is also possible to install using pip with `pip install -e .`, and bypass Poetry.

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
vector2dggs h3 -v DEBUG -id ogc_fid -r 9 -p 5 -t 4 --overwrite -lyr topo50_lake postgresql://user:password@host:port/db ./topo50_lake.parquet
```

## Citation

```bibtex
@software{vector2dggs,
  title={{vector2dggs}},
  author={Ardo, James and Law, Richard},
  url={https://github.com/manaakiwhenua/vector2dggs},
  version={0.10.0},
  date={2023-04-20}
}
```

APA/Harvard

> Ardo, J., & Law, R. (2023). vector2dggs (0.10.0) [Computer software]. https://github.com/manaakiwhenua/vector2dggs

[![manaakiwhenua-standards](https://github.com/manaakiwhenua/vector2dggs/workflows/manaakiwhenua-standards/badge.svg)](https://github.com/manaakiwhenua/manaakiwhenua-standards)
