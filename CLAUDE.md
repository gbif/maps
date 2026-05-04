# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
# Build all modules
mvn clean install

# Build and deploy (as used in CI)
mvn clean verify deploy -B -U

# Build a specific module
mvn clean install -pl vectortile-server

# Build vectortile-server with environment profile (required for deployment)
mvn clean package -U -Pvectortile

# Run the vectortile-server
java -jar vectortile-server/target/vectortile-server-*-SNAPSHOT.jar

# Run all tests
mvn test

# Run a single test class
mvn test -pl common -Dtest=VectorTileFiltersTest

# Run a single test method
mvn test -pl common -Dtest=VectorTileFiltersTest#testMethod

# Integration tests (named *IT.java) run via failsafe, not surefire
mvn verify -pl vectortile-server
```

## Module Structure

This is a Maven multi-module project. The modules and their roles:

- **`common`** — Shared library: tile projections (EPSG), vector tile filtering, HBase row key salting, ZooKeeper metastore client, hex/square binning. Contains the two Protobuf schemas (`point_feature.proto`, `tile_feature.proto`) for HBase storage.
- **`vectortile-common`** — Shared Spring/REST code reused by both tile servers: `Config.java` (HBase + ES config POJOs), `AdHocMapsResource` base class, `Params` utilities, OpenAPI annotations.
- **`vectortile-server`** — Spring Boot server serving occurrence density maps. Two data sources: HBase (pre-built "simple maps") and ElasticSearch (ad hoc queries). Entry point: `TileServerApplication`.
- **`event-vectortile-server`** — Spring Boot server for event-based maps. Mirrors the occurrence server structure; uses `AdHocEventMapsResource` instead.
- **`spark-generate-maps`** — Spark batch job that reads from Hive (`occurrence` table) and builds HBase tile pyramids. Main entry points: `Backfill` (called from Airflow with `configFile timestamp` args), `PrepareBackfill`, `FinaliseBackfill`.
- **`mapnik-server`** — Docker-based PNG renderer: accepts vector tiles and renders them via Mapnik. Built with Maven, run via Docker.

## Architecture

### Two map types
1. **Simple maps** (high-performance): Pre-built by the Spark job from the full occurrence dataset. Stored in HBase as Protobuf-encoded tile pyramids, keyed by map type (taxonKey, datasetKey, country, etc.). Filterable only by `basisOfRecord` and `year` at serve time.
2. **Ad hoc maps** (full-search): Live ElasticSearch geohash aggregation queries. Supports the full occurrence search API filters. Significantly slower.

### URL patterns (context path `/map/`)
- Simple: `GET /occurrence/density/{z}/{x}/{y}.mvt?taxonKey=212`
- Ad hoc: `GET /occurrence/density/adhoc/{z}/{x}/{y}.mvt?taxonKey=212&hasGeospatialIssue=true`

### HBase storage
- Two table types: **points table** (raw `PointFeatures` protobuf, individual occurrences) and **tiles table** (pre-aggregated `TileFeatures` protobuf, pixel-precision counts per BasisOfRecord/year).
- Row keys are salted with a modulus hash (`ModulusSalt`) to distribute load. Salt moduli for points and tiles are configured separately and must match between the Spark build and the tile server.
- The `MapMetastore` interface (backed by ZooKeeper) stores which HBase table names are currently active, enabling hot-swapping tables during backfills without downtime.

### Supported projections
Four EPSG codes are supported (factory: `Tiles.fromEPSG`):
- `EPSG:3857` — Web Mercator (default web maps)
- `EPSG:4326` — WGS84 (two-tile world)
- `EPSG:3575` — North Pole Lambert Azimuthal Equal Area
- `EPSG:3031` — Antarctic Polar Stereographic

### Protobuf
Protobuf schemas live in `common/src/main/proto/`. The protoc version must match `<protoc.version>` in the root `pom.xml` (currently 4.33.4). See the root `README.md` for installation steps. Generated Java sources are in `common/src/main/java/org/gbif/maps/io/`.

### Spark backfill workflow
`MapBuilder` (called by `Backfill`) reads from a Hive view, runs UDFs to compute tile coordinates and HBase row keys, then writes HBase bulk-load files. Mode is either `tiles` or `points`. `FinaliseBackfill` triggers a ZooKeeper metastore update to point the tile servers at the new tables.

## Key Design Decisions

- **Salt modulus** values in HBase config must stay in sync between Spark (write) and vectortile-server (read). Changing them requires a full backfill.
- **`esOccurrenceConfiguration.enabled`** flag gates all ElasticSearch beans and the ad hoc endpoint. The `es-only` Spring profile disables the HBase bean entirely.
- **Cache2k** is used for in-process tile and point caching in `HBaseMaps`. Cache configuration is injected as `Cache2kConfig` beans.
- The Spark SQL query in `MapBuilder.SQL_TEXT` always filters `hasGeospatialIssues = false` and `occurrenceStatus = 'PRESENT'`; simple maps therefore never include records with geospatial issues.
