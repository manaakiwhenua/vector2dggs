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
  -c, --cut_threshold INTEGER     Cutting up large polygons into target length(meters) 
                                  [default: 5000; required]
  -t, --threads INTEGER           Amount of threads used for operation
                                  [default: 7]
  --help                          Show this message and exit.
```
## Example commands
```bash
python vector2dggs.py /vector2dggs/nz-lake-polygons-topo-150k.gpkg ./lakes_nrc -r 13 -p 150 -c 5000 -t 15
```