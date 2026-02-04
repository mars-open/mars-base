# MARS-Base

Positionpoints serve as the **base technology** for efficient maintenance applications provided by MARS.
This repository contains geospatial data pipelines for generating and managing Positionpoints (PPs) for railway infrastructure.

## Overview

MARS-Base processes railway network data to create precise positionpoints along tracks. 
The system uses modern geospatial technologies like H3 and Apache Sedona, and outputs data in optimized formats for web-based visualization.

Declarative data pipelines implemented using Spark and SmartDataLakeBuilder ensure reproducibility and flexibility.

Currently data pipelines for the following countries are implemented:
- Switzerland: swissTopo TLM3d dataset 
- Germany: Infrastukturregister service

## Features

- **Efficient Position Point Generation**: Creates PPs along railway tracks with configurable spacing
- **Switch and Turnout Support**: Handles complex railway geometry including switches and turnouts
- **Token identifier**: Base32 token system for unique PP identification
- **Efficient Data Formats**: Outputs to PMTiles and FlatGeobuf for optimal performance

## Architecture Decisions

### Spatial Indexing: H3 vs S2 vs A5 vs GeoHash

After evaluation, **H3 geospatial grid library** is used for spatial indexing. Key considerations:
- efficient spacial joins
- efficient neighbor cell searches
- hexagonal grid structure reduces neighbors from 8 to 6
- cell size for finest resolution 15 is about 1m2, which is good to cover single tracks and minimizes local records when joining different sources by cell id
- coarser resolutions can be tested for potential performance improvements

Comparision: [Location-Based Algorithms Comparison](https://medium.com/@sylvain.tiset/breaking-down-location-based-algorithms-r-tree-geohash-s2-and-h3-explained-a65cd10bd3a9)
H3 documentation: [H3 Documentation](https://h3geo.org/docs/)

### PP Creation Priority

Position points in a round based algorithm with the following priority:
1. Main tracks before turnouts
2. Track-by-track processing

These priorities ensure consistent PP placement in switches, e.g. positionpoints are created on main track in the central zone of the switch.

In every round PPs created in neighbouring cells are considered to create new PPs in the selected cell.
This ensures that no duplicate PPs are created, even if they are close to the border of a cell.

### Positionpoint ID 

The ID of a positionpoint is composed of
- base cell number and detail digits of h3 Cell id level 15 (52 bits)
- numbering of the Positionpoints within this cell (12 bits = max 4096 PPs per cell)

The result is a 64bit integer number, which allows for efficient joins, agggregations and storage.

#### Azimuth exclusion from PP ID

Azimuth (direction) is an attribute of a Positionpoint, but not included in the PP identifier:
- The azimuth of the track just terminates the lifecycle of the track, but not of the underground
- Having a PP identified without azimuth allows to keep long-lived underground investigations

### Token identifier

A token is an easier to read identifier for positionpoints, compared to a 64bit Integer number.

**Base32 (13 characters)** was chosen over Base58 (11 characters), Base64 and Hexadecimal representation:
- Better readability (avoids ambiguous characters)
- Good tool support
- Acceptable length trade-off

### PMTiles format

Pmtiles format is used for efficient web visualization of large vector datasets, e.g. Positionpoints.
As it is a tiled format, only the required tiles are loaded based on the current map view.
- **Uncompressed format**: Used due to MapLibre compatibility requirements
- **Sparse zoom levels**: Only selected zoom levels are included to optimize file size and loading times

Details see [PMTiles Discussion](https://github.com/protomaps/PMTiles/discussions/591#discussioncomment-14149097))

Viewer: https://pmtiles.io/

### FlatGeobuf

FlatGeobuf is used for efficient storage and retrieval of geospatial vector data, e.g. edge geometries.
- much more efficient format than GeoJSON

Viewer: https://geodataviewer.com/studio/

## Known Limitations

- **PP Mapping Direction**: May be null when azimuth difference exceeds 45 degrees (manual correction may be needed)
- **PP Mapping Position**: Can exceed edge boundaries when snapped to existing PPs
- **Incremental Updates**: Current pipelines do not yet support incremental updates

## License

Open Source - ensure AI rewrites to other languages remain open source.

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request with your changes.
