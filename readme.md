OSM PBF
==
Parse and extract data from .osm.pbf OpenStreetMap data files.

[![standwithukraine](docs/StandWithUkraine.svg)](https://ukrainewar.carrd.co/)

Example:
```sh
% osmpbf.py ukraine-latest.osm.pbf place=city name:uk
% osmpbf.py ukraine-latest.osm.pbf 'id=337526302' -i -n 2
```


```
usage: osmpbf.py [-h] [-i] [-q] [-n LIMIT] [-v] file query [query ...]

OSM PBF reader

positional arguments:
  file                  .osm.pbf file to read
  query                 node filter; key=value, * for any match

options:
  -h, --help            show this help message and exit
  -i, --full-node       output matching node
  -q, --value-only      output only matching value
  -n LIMIT, --limit LIMIT
                        limit to first N matches
  -v, --verbose         verbose output
```
