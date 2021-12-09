The tiles in this folder were captured to support https://github.com/gbif/maps/issues/48 using the following

```
curl 'https://api.gbif.org/v2/map/occurrence/density/3/8/2.mvt?srs=EPSG:4326&publishingCountry=FR&verbose=true' > 3_8_2.mvt
curl 'https://api.gbif.org/v2/map/occurrence/density/4/16/4.mvt?srs=EPSG:4326&publishingCountry=FR&verbose=true' > 4_16_4.mvt
curl 'https://api.gbif.org/v2/map/occurrence/density/4/17/4.mvt?srs=EPSG:4326&publishingCountry=FR&verbose=true' > 4_17_4.mvt
curl 'https://api.gbif.org/v2/map/occurrence/density/4/16/5.mvt?srs=EPSG:4326&publishingCountry=FR&verbose=true' > 4_16_5.mvt
curl 'https://api.gbif.org/v2/map/occurrence/density/4/17/5.mvt?srs=EPSG:4326&publishingCountry=FR&verbose=true' > 4_17_5.mvt
```

And using the utility to read straight from HBase
```
java ExportRawTile c5zk1.gbif.org prod_h_maps_tiles_20211208_1900 100 EPSG_4326 3 8 2 publishingCountry=FR /tmp/publishingCountry-FR-3-8-2.mvt

```



