#!/bin/bash

inputfile=$1
region=$( [[ $inputfile =~ ^([a-zA-Z0-9]+) ]] && echo "${BASH_REMATCH[1]}" )
outputfile="$region-railway-tracks.jsons"
echo .
echo "creating output file $outputfile..."
echo .

echo "Filtering railway tracks from OSM data..."
osmium tags-filter $inputfile w/railway=rail,tram,subway,narrow_gauge --overwrite -o railway.pbf
echo .

echo "Exporting filtered railway tracks to GeoJSON sequence format..."
osmium export railway.pbf --geometry-types linestring -f geojsonseq --format-option=print_record_separator=false --overwrite -o $outputfile -e
echo "done"

rm railway.pbf