# MARS-Base

Positionpoints serve as the **base technology** for efficient maintenance applications provided by MARS.
This repository contains geospatial data pipelines for generating and managing Positionpoints (PPs) for railway infrastructure.

## Overview

MARS-Base processes railway network data to create precise positionpoints along tracks. 
The system uses modern geospatial technologies like H3 and Apache Sedona, and outputs data in optimized formats for web-based visualization.

Declarative data pipelines implemented using Spark and SmartDataLakeBuilder ensure quality and flexibility.

Currently data pipelines for the following countries are implemented:
- Switzerland: swissTopo TLM3d dataset
- Germany: Infrastukturregister service; unfortunately data quality is not sufficient for creating PPs
- OpenRailwayMap: global coverage with promising results in Germany

## Features

- **Efficient Position Point Generation**: Creates PPs along railway tracks with configurable spacing
- **Switch and Turnout Support**: Handles complex railway geometry including switches and turnouts
- **Token identifier**: Base32 token for unique, short and easy to read PP identification
- **Efficient Data Formats**: Outputs to PMTiles and FlatGeobuf for optimal performance and direct use in Webapplications

## Getting Started

TODO: Add instructions for setting up the environment, configuring and running pipelines.

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

Position points are created in a multi-round-based algorithm with the following priority:
1. Main tracks before turnouts
2. Track-by-track processing to avoid creating duplicates

These priorities ensure consistent PP placement in switches, e.g. positionpoints are created on main track in the central zone of the switch.

In every round PPs created in neighbouring cells are included in the nearest neighbour search to create new PPs in the selected cell.
This ensures that no duplicate PPs are created, even if they are close to the border of a cell.

### Positionpoint ID 

The ID of a positionpoint is composed of
- base cell number and detail digits of H3 Cell ID level 15 (52 bits) 
- numbering of the Positionpoints within this cell (12 bits = max 4096 PPs per cell)

The result is a 64bit integer number, which allows for efficient joins, aggregations and storage.

#### Azimuth exclusion from PP ID

Azimuth (direction) is an attribute of a Positionpoint, but not included in the PP identifier:
- The azimuth of the track just terminates the lifecycle of the track, but not of the underground
- Having a PP identified without azimuth allows to keep all data for a given location together, even if the track direction changes
- In case of track direction change, the direction on the PP mapping will be set to Null. This allows to identify these cases, create a metric for it and potentially correct the direction manually if needed.

### Token identifier

A token is an easier to read identifier for positionpoints, compared to a 64bit Integer number.

**Base32 (13 characters length)** was chosen over Base58 (11 characters length), Base64 and Hexadecimal representation:
- Better readability (avoids ambiguous characters)
- Good tool support
- Acceptable length trade-off

### Zoom levels

Positionpoints get a zoom level assigned to be able to filter a grid resolution efficiently:
- 0: endpoints of edges
- 1: every 10m
- 2: every 1m
- 3: configured base resolution (PP distance), e.g. 25cm by default

### Positionpoint creation along edges

Following the idea that a positionpoint represents a small section of the track (e.g. +-12.5cm), the position of the first PP is not at the beginning of the edge, but at the middle of the first PP section, e.g. at 12.5cm for covering position 0cm-25cm of the edge.
Normally there is some left-over space as the edge length is not an exact multiple of the PP distance.
To keep the raster of the PPs consistent, the left-over space is distributed equally at the beginning and end of the edge, so that the first PP starts earliest on position 12.5cm, but latest before 0.25cm.

### PMTile file format

PMTiles format is used for efficient web visualization of large vector datasets, e.g. Positionpoints.
As it is a tiled format, only the required tiles are loaded based on the current map view.
- **Uncompressed format**: Used due to MapLibre compatibility requirements
- **Sparse zoom levels**: Only selected zoom levels are included to optimize file size and loading times

Viewer: https://pmtiles.io/

### FlatGeobuf file format

FlatGeobuf is used for efficient storage and retrieval of geospatial vector data, e.g. edge geometries.
- much more efficient than GeoJSON

Viewer: https://geodataviewer.com/studio/

## Known Limitations

- **PP Mapping Direction**: May be null when azimuth difference exceeds 45 degrees (manual correction may be needed)
- **PP Mapping Position**: Can exceed edge boundaries when snapped to existing PPs
- **Incremental Updates**: Current pipelines do not yet support incremental updates

## License

MARS Base is distributed under the GNU General Public License version 3 or later.
The full terms of the license can be found at https://www.gnu.org/licenses/gpl-3.0.en.html.

For this repository the GPLv3 is explicitly applied also to AI translations of the code to other programming languages, overruling any potentially different interpretation.

## Contributors

![ELCA AG](/.assets/elca.png)

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request with your changes.

