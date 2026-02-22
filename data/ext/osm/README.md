
# Prepare OSM Data

## Preconditions

- Install osmium-tool: sudo apt-get install osmium-tool

## Download

- download .osm.pbf file from https://download.geofabrik.de/europe.html

## Extract and export to GeoJson 

- run `./export.sh <input.osm.pbf>`

Note that the first word of the input file name is extracted as OSM region, e.g. switzerland.
