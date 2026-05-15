#!/bin/bash

inputfile=$1
region=$( [[ $inputfile =~ ^([a-zA-Z0-9]+) ]] && echo "${BASH_REMATCH[1]}" )
outputfileTracks="$region-railway-tracks.jsons"
outputfileSwitches="$region-railway-switches.jsons"
echo .
echo "creating output file $outputfileTracks..."
echo "Filtering railway tracks from OSM data..."
osmium tags-filter $inputfile "w/railway=rail,tram,subway,narrow_gauge" --overwrite -o railway.pbf
echo "Exporting filtered railway tracks to GeoJSON sequence format..."
osmium export railway.pbf -c export-config.json --geometry-types linestring -f geojsonseq --format-option=print_record_separator=false --overwrite -o $outputfileTracks -e
echo "done"

rm railway.pbf

echo .
echo "creating output file $outputfileSwitches..."
echo "Filtering railway switches from OSM data..."
osmium tags-filter $inputfile "n/railway=switch" --overwrite -o railway.pbf
echo "Exporting filtered railway tracks to GeoJSON sequence format..."
osmium export railway.pbf -c export-config.json --geometry-types point -f geojsonseq --format-option=print_record_separator=false --overwrite -o $outputfileSwitches -e
echo "done"

rm railway.pbf
