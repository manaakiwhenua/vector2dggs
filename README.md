# vector2dggs
Vector equivalent of raster2dggs, also only supports H3 DGGS.

## Usage

```
Usage: vector2dggs.py [OPTIONS] INPUT_FILE OUTPUT_FILE

Options:
  -v, --verbosity LVL             Either CRITICAL, ERROR, WARNING, INFO or
                                  DEBUG  [default: INFO]
  -r, --resolution [0|1|2|3|4|5|6|7|8|9|10|11|12|13|14|15]
                                  H3 resolution to index  [required]
  -p, --partitions TEXT           Geo-partitioning, currently only available
                                  in Hilbert method  [default: 50; required]
  -c, --cut_threshold INTEGER     Cutting up large polygons into target length 
                                  [default: 5000; required]
  -t, --threads INTEGER           Amount of threads used for operation
                                  [default: 7]
  --help                          Show this message and exit.
```
## Example commands
```bash
python vector2dggs.py /vector2dggs/nz-lake-polygons-topo-150k.gpkg ./lakes_nrc -r 13 -p 50 -cc True -ccr 3 -t 15
```

## Outputs
Currently outputs n(partitions) amount of parquet files that can be opened using dask.
To visualize output:
```python
>>> import dask.dataframe as dd
>>> import h3pandas
>>> df = dd.read_parquet('./lakes_nrc').compute()
>>> df.h3.h3_to_geo_boundary().to_file('lakes_nrc.gpkg', driver='GPKG')
```