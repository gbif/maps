## Maps
 
GBIF Maps in Mapbox Vector Tile (MVT) format and as PNGs.

 - Processing from HBase or Parquet files into the tile pyramid, stored in HBase
 - Rendering in Mapbox vector tile (MVT) format from HBase or SOLR data sources
 - Mapnik as an optional view to convert MVTs into PNGs
 - Hexagon binning views
 - Fast disaster recovery and batch processing
 - Year resolution time series data
 
The general architecture:

![Architecture](./assets/architecture.png)
