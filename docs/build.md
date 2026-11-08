# Build & Run

## Prerequisites

- Java JDK 17 or newer (CI uses JDK 21)
- Maven
- For input data preparation: 
  - [osmium-tool](https://osmcode.org/osmium-tool/) (OpenStreetMap)
  - QGIS or Python/GeoPandas (swissTopo TLM3D)

The project is built with Scala 2.13 and Spark 4.1. Its parent POM is SmartDataLakeBuilder 3.x (`io.smartdatalake:sdl-parent`), which pins the Spark, Delta and Hadoop versions.

## Build and test

```bash
mvn -B clean package                     # compile + unit tests + jar
mvn test                                 # unit tests only
mvn test -Dsuites=net.mars.open.pp.SnapPpTransformerTest   # single suite
mvn test -Dsuites='net.mars.open.pp.CreatePpTransformerTest EdgePoints'  # tests whose name contains "EdgePoints"
```

- Maven runs only test classes with suffix `Test`. Integration tests with suffix `IT` (e.g.  `PipelineIT`, an SDLB dry run of feed `ch`) must be run from the IDE.
- Every source file must carry the GPLv3 header from `src/license/gplv3-header.txt` (checked by `license-maven-plugin`).
- Code is formatted with scalafmt (`.scalafmt.conf`).

## Prepare input data

Input data is prepared outside of Spark and placed under `data/ext`:

- **OpenStreetMap**: download an `.osm.pbf` file from [Geofabrik](https://download.geofabrik.de/europe.html) and run `data/ext/osm/export.sh <region>.osm.pbf`.
  The first word of the file name is used as OSM region, e.g. `switzerland`. See [data/ext/osm/README.md](../data/ext/osm/README.md).
- **swissTopo TLM3D**: download the GeoPackage from [swissTopo](https://www.swisstopo.admin.ch/de/landschaftsmodell-swisstlm3d) and convert it to GeoParquet with QGIS or the GeoPandas script.
  See [data/ext/tlm3d/README.md](../data/ext/tlm3d/README.md).

## Configuration

Pipelines are configured in HOCON files:

- `config/`: pipeline definitions. `global.conf` selects the active country with `parameters = ${parametersXX}`; `ch.conf` and `de.conf` hold the country parameters.
- `envConfig/dev.conf`: local development environment. Data under `./data`, geometries uploaded to S3, S3 credentials read from `~/.key/s3`.
- `envConfig/ci.conf`: CI environment. Inputs read from S3, credentials from environment variables `S3_ACCESS_KEY` / `S3_SECRET_KEY`.

## Run pipelines

Pipelines run with SDLB's `DefaultSmartDataLakeBuilder`, selecting actions by feed name (`tlm3d-ext`, `tlm3d`, `osm`, `osm-line`, `pp`, `ch`, `de`, ...).
Spark on Java 17+ needs a set of `--add-opens` JVM flags; copy the full `JAVA_OPTIONS` from [.github/workflows/ui-build.yml](../.github/workflows/ui-build.yml).

Example: create the Swiss Positionpoints from swissTopo TLM3D, as CI does:

```bash
JAVA_OPTIONS="-Xmx8g --add-opens=java.base/sun.nio.ch=ALL-UNNAMED ..."   # see ui-build.yml
mvn -B exec:exec -Dexec.executable="java" \
  -Dexec.args="$JAVA_OPTIONS -cp %classpath io.smartdatalake.app.DefaultSmartDataLakeBuilder --feed-sel tlm3d-ext,tlm3d,pp,ch --config ./config,./envConfig/dev.conf -n create-pp --parallelism 2"
```

Add `--state-path <dir>` to record the run state for the SDLB Visualizer.

## Inspect the results

- PMTiles: https://pmtiles.io/ or the [pmtiles CLI](https://docs.protomaps.com/pmtiles/cli), e.g. `pmtiles show <url>`
- FlatGeobuf: https://geodataviewer.com/studio/ or GDAL `ogrinfo`
- Positionpoints on a map: [mars-explorer](https://github.com/mars-open/mars-explorer)

## CI

On push to `master` and weekly, CI builds the project, runs the Swiss pipeline against S3, commits the SDLB run state to `viz/state/`,
exports schemas and configuration, and deploys the SDLB Visualizer to GitHub Pages (https://base-pipelines.mars-open.net/).
