# GBIF Maps

GBIF Occurrence Maps in [Mapbox Vector Tile](https://www.mapbox.com/vector-tiles/specification/) (MVT) format and as PNGs.

 - Processing from HBase or Parquet files into the tile pyramid, stored in HBase
 - Rendering in Mapbox vector tile (MVT) format from HBase or SOLR data sources
 - Mapnik as an optional view to convert MVTs into PNGs
 - Hexagon binning views
 - Fast disaster recovery and batch processing
 - Year resolution time series data
 
The general architecture:

![Architecture](./assets/architecture.png)

## Debugging interfaces

There are debug/demo interfaces:

* [Vector tiles](https://api.gbif-dev.org/v2/map/debug/ol/)
* [Raster tiles](https://api.gbif-dev.org/v2/map/demo.html)

TODO: Update to externally available servers, once the deployment is completed.
