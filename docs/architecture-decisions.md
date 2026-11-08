# Architecture Decisions

For the business context see [Business Motivation](motivation.md).

## Technology stack

Declarative data pipelines implemented with Spark and [SmartDataLakeBuilder](https://www.smartdatalake.io) (SDLB) ensure quality and flexibility.
Geospatial processing uses [Apache Sedona](https://sedona.apache.org/) (JTS geometries) and [H3](https://h3geo.org/) indexing.
Pipelines follow a medallion layout:
1. bronze: raw inputs (`ext-*`)
2. silver: Delta tables (`slv-*`)
3. gold: outputs (`gld-*`: Delta, GeoParquet, FlatGeobuf, PMTiles).

The pipeline documentation can be browsed in the SDLB Visualizer under https://base-pipelines.mars-open.net/.

## Spatial Indexing: H3 vs S2 vs A5 vs GeoHash

After evaluation, the **H3 geospatial grid library** is used for spatial indexing. Key considerations:
- efficient spatial joins
- efficient neighbor cell searches
- hexagonal grid structure reduces neighbors from 8 to 6
- cell size at the finest resolution 15 is about 1m², which is good to cover single tracks and minimizes local records when joining different sources by cell id
- coarser resolutions can be tested for potential performance improvements
- world wide coverage

Comparison: [Location-Based Algorithms Comparison](https://medium.com/@sylvain.tiset/breaking-down-location-based-algorithms-r-tree-geohash-s2-and-h3-explained-a65cd10bd3a9)
H3 documentation: [H3 Documentation](https://h3geo.org/docs/)

## PP Creation Priority

Positionpoints are created in a multi-round algorithm with the following priority:
1. Main tracks before turnouts
2. Track-by-track processing to avoid creating duplicates

These priorities ensure consistent PP placement in switches, e.g. Positionpoints are created on the main track in the central zone of the switch.

In every round, PPs created in neighboring cells are included in the nearest neighbor search to create new PPs in the selected cell.
This ensures that no duplicate PPs are created, even if they are close to the border of a cell.

## Positionpoint ID

The ID of a Positionpoint is composed of
- the base cell number and detail digits of the H3 cell ID at resolution 15 (52 bits)
- numbering of the Positionpoints within this cell (12 bits = max 4096 PPs per cell)

The result is a 64-bit integer, which allows for efficient joins, aggregations and storage.

### Azimuth exclusion from PP ID

Azimuth (direction) is an attribute of a Positionpoint, but not included in the PP identifier:
- A change of the track's azimuth terminates the lifecycle of the track, but not of the location underneath
- Identifying a PP without azimuth keeps all data for a given location together, even if the track direction changes
- In case of a track direction change, the direction on the PP mapping is set to null. This allows to identify these cases, create a metric for them and potentially correct the direction manually if needed.

## Token identifier

A token is an identifier for Positionpoints, that is easier to read than a 64-bit integer number.

**Base32 (13 characters)** was chosen over Base58 (11 characters), Base64 and hexadecimal representation:
- Better readability (avoids ambiguous characters)
- Good tool support
- Acceptable length trade-off

## Zoom levels

Positionpoints get a zoom level assigned to be able to filter a grid resolution efficiently:
- 0: endpoints of edges
- 1: every 10m
- 2: every 1m
- 3: configured base resolution (PP distance), e.g. 25cm by default

## Positionpoint creation along edges

Following the idea that a Positionpoint represents a small section of the track (e.g. ±12.5cm), the first PP is not placed at the beginning of the edge, but in the middle of the first PP section, e.g. at 12.5cm for covering position 0cm-25cm of the edge.
Normally there is some left-over space, as the edge length is not an exact multiple of the PP distance.
To keep the raster of the PPs consistent, the left-over space is distributed equally at the beginning and end of the edge, so that the first PP is placed at 12.5cm at the earliest, but before 25cm at the latest.

## PMTiles file format

The PMTiles format is used for efficient web visualization of large vector datasets, e.g. Positionpoints.
As it is a tiled format, only the required tiles are loaded based on the current map view.
- **Uncompressed format**: used due to MapLibre compatibility requirements
- **Sparse zoom levels**: only selected zoom levels are included to optimize file size and loading times

Viewer: https://pmtiles.io/

## FlatGeobuf file format

FlatGeobuf is used for efficient storage and retrieval of geospatial vector data, e.g. edge geometries.
It is much more efficient than GeoJSON.

Viewer: https://geodataviewer.com/studio/

## Known Limitations

The reference implementation has the following limitiations to keep in mind:

- **PP Mapping Direction**: may be null when the azimuth difference exceeds 45 degrees (manual correction may be needed)
- **PP Mapping Position**: can exceed edge boundaries when snapped to existing PPs
- **Incremental Updates**: current pipelines do not yet support incremental updates
