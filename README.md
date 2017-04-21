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

* [Vector tiles](https://api.gbif.org/v2/map/debug/ol/)
* [Vector tiles (Web Mercator only)](https://api.gbif.org/v2/map/debug/)
* [Raster tiles](https://api.gbif.org/v2/map/demo.html)

## The result

<p align="center"><img src="https://api.gbif.org/v2/map/occurrence/density/0/0/0@1x.png?srs=EPSG:3857&style=purpleYellow.point" width="384" /></p>

<p align="center"><img src="https://api.gbif.org/v2/map/occurrence/density/0/0/0@1x.png?srs=EPSG:4326&style=purpleYellow.point" width="384" /><img src="https://api.gbif.org/v2/map/occurrence/density/0/1/0@1x.png?srs=EPSG:4326&style=purpleYellow.point" width="384" /></p>

<p align="center"><img src="https://api.gbif.org/v2/map/occurrence/density/0/0/0@1x.png?srs=EPSG:3575&style=purpleYellow.point" width="384" /> <img src="https://api.gbif.org/v2/map/occurrence/density/0/0/0@1x.png?srs=EPSG:3031&style=purpleYellow.point" width="384" /></p>
