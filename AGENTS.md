# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

MARS-Base builds **Positionpoints (PPs)** — stable, uniquely identified points at fixed spacing (default 25cm) along railway tracks — from public railway network data (Swiss swissTopo TLM3D, OpenStreetMap/OpenRailwayMap for Germany and others). 
Pipelines are declarative [SmartDataLakeBuilder](https://www.smartdatalake.io) (SDLB) configs (HOCON in `config/`) that call custom Scala/Spark transformers and DataObjects in `src/main/scala`. 
Geospatial work uses Apache Sedona (JTS geometries) and H3 indexing. README.md holds the design rationale (H3 choice, PP ID layout, token, zoom levels, edge spacing rules) — read it before changing PP creation or ID logic.

## Build & test

Maven, Scala 2.13, Spark 4.1, Java 17 target (CI uses JDK 21). 
The parent POM is SDLB 3.x (`io.smartdatalake:sdl-parent`), which pins Spark/Delta/Hadoop versions; Spark DataObjects/Actions come from `sdl-spark`. 
Env-specific Spark connection configurations come from `env.sparkMaster` / `env.sparkOptions` in `envConfig/*.conf` (SDLB forbids defining the same connection in two files). 
SDLB 3 doesn't create databases and the local catalog is in-memory, so `env.slvDb`/`env.gldDb` are `default` locally and in CI.

```bash
mvn -B clean package                     # compile + unit tests + jar
mvn test                                 # unit tests only
mvn test -Dsuites=net.mars.open.pp.SnapPpTransformerTest   # single suite
mvn test -Dsuites='net.mars.open.pp.CreatePpTransformerTest EdgePoints'  # tests whose name contains "EdgePoints"
```

- scalatest-maven-plugin runs only classes with suffix `Test`; `*IT` classes (e.g. `PipelineIT`, an SDLB dry-run of feed `ch`) are not run by Maven — run them from the IDE. Tests get `ENV_CONFIG=ci.conf`.
- `src/test/scala/net/mars/open/Sandbox.scala` is a scratch area, not a real test.
- Every source file must carry the GPLv3 header from `src/license/gplv3-header.txt` (enforced by `license-maven-plugin`, inherited from the parent POM).
- Formatting: scalafmt (`.scalafmt.conf`, maxColumn 160).

## Running pipelines

Pipelines run via `io.smartdatalake.app.DefaultSmartDataLakeBuilder` with `--config ./config,./envConfig/<env>.conf` and a feed selector.
CI's invocation (`.github/workflows/ui-build.yml`):

```bash
mvn -B exec:exec -Dexec.executable="java" \
  -Dexec.args="$JAVA_OPTIONS -cp %classpath io.smartdatalake.app.DefaultSmartDataLakeBuilder --feed-sel tlm3d-ext,tlm3d,pp,ch --config ./config,./envConfig/ci.conf --state-path viz/state -n create-pp --parallelism 2"
```

`JAVA_OPTIONS` needs the Spark `--add-opens` flags and `-Xmx8g` (copy them from the workflow). Feed names come from each action's `metadata.feed` (`tlm3d-ext`, `tlm3d`, `osm`, `osm-line`, `ch`, `de`, ...).

Environments (`envConfig/`):
- `dev.conf`: local data under `./data`, inputs under `./data/ext`, geometries uploaded to S3; S3 credentials read from `~/.key/s3` via `FileSecretProvider`.
- `ci.conf`: inputs from S3, credentials from env vars `S3_ACCESS_KEY`/`S3_SECRET_KEY`, and `ppPmtilesFilter` restricts PMTiles output to a subset for speed.

Input data preparation is outside Spark: `data/ext/osm/export.sh <region>.osm.pbf` (needs osmium-tool; first word of filename = OSM region) and `data/ext/tlm3d/` (GPKG → GeoParquet via QGIS or GeoPandas script).

## Pipeline architecture

Medallion layout: `ext-*` (raw inputs) → `slv-*` (Delta tables, `slv-delta` connection) → `gld-*` (outputs: Delta tables, GeoParquet, FlatGeobuf `.fgb`, PMTiles). Delta table templates `slvTable`/`gldTable` are in `config/global.conf`.

Config files:
- `global.conf` — Spark/Sedona/Delta extensions, UDF registration, connections, templates, and `parameters = ${parametersXX}` which **selects the active country**. Some actions reference the generic `${parameters...}`, others the country-specific `${parametersCH...}`/`${parametersDE...}` directly.
- `ch.conf` / `de.conf` — country parameters (`countryCode`, metric `defaultSrcCrs` e.g. EPSG:2056 / EPSG:25832, `topology` source, OSM region) and the country's snapping + gold outputs.
- `ch-tlm3d.conf`, `osm.conf` — source-specific topology pipelines. `de-isr.conf.disabled` is inactive.
- `pp.conf` — the shared `slv-pp` table (all PPs across countries/sources).

Generic per-source flow (each step a `CustomDataFrameAction` with a `ScalaClassSparkDfsTransformer`):
1. Source-specific prep transformer (`ch/tlm3d/Tlm3dEdgeInputTransformer`, `osm/OsmTrackTransformer`, `osm/OsmSwitchTransformer`) normalizes tracks/switches.
2. `pp/EdgeTransformer` → edges; `pp/NodeTransformer` → nodes (edge endpoints, switches).
3. `pp/CreatePpTransformer` → PP *candidates* at fixed intervals per edge, with priority (main track before turnout), azimuth, grade, zoom level. Requires metric CRS (`srcCrs`).
4. `pp/SnapPpTransformer` (country action, e.g. `create-slv-de-pp`) snaps candidates to existing PPs in `slv-pp`, creating new PPs only where none exist, and writes a PP→edge mapping table. It is H3-cell-partitioned and runs multiple rounds per priority/edge, always including neighbor cells, so that no duplicates appear at cell borders. `slv-pp` is a **recursive input** (read and appended in the same action, `saveMode = Append`, `schemaMin` from case class `net.mars.open.pp.Pp`). IDs come from `PpIdGenerator` (H3 res-15 cell bits + 12-bit in-cell counter).
5. Gold actions join `slv-pp` + mapping + source PPs into outputs; `CrsTransformer` reprojects to EPSG:4326 for FGB/PMTiles.

Transformer options in config (`options = {...}`) map to the `transform(...)` method parameters by name; `renamedInputIds`/`renamedOutputIds` map DataObject IDs to the parameter names the transformer expects (e.g. `edge`, `node`, `pp-existing`, `pp-new`).

Custom DataObjects (used as `type = net.mars.open.pp.<Class>` in config):
- `PMTilesDataObject` — writes vector tiles; DataFrame needs `geometry` (EPSG:4326), `layer`, optional `id`. Uses `io/tileverse/pmtiles/TileverseAccessor.scala` to reach package-private tileverse APIs (same trick as `org/apache/spark/sql/custom/MarsSparkAccessor.scala` for Spark internals).
- `FlatGeobufDataObject` — writes `.fgb` to `localPath` and uploads to `path` (S3).
- `de/isr/WfsDataObject` — WFS reader for the disabled ISR source.

## Visualizer / CI

CI (push to master, weekly schedule) builds, runs the CH pipeline against S3, commits SDLB run state to `viz/state/` (so those files are machine-generated — don't hand-edit), exports schemas/config, and deploys the SDLB visualizer to GitHub Pages (https://base-pipelines.mars-open.net/).
The Maven profiles `export-config` and `generate-catalog` wrap SDLB's config exporter and lab catalog generator.
