To create a Parquet file source (if not importing directly from HBase)
```
CREATE TABLE tim.occurrence_map_source STORED AS parquet AS
SELECT 
  decimalLatitude, decimalLongitude,
  datasetKey, publishingOrgKey, countryCode, publishingCountry,
  kingdomKey, phylumKey, classKey, orderKey, familyKey, genusKey, speciesKey, taxonKey,
  year, basisOfRecord
FROM
  prod_b.occurrence_hdfs
WHERE
  decimalLatitude IS NOT NULL AND decimalLongitude IS NOT NULL AND hasGeospatialIssues=false;
  
CREATE TABLE tim.occurrence_map_source_sample STORED AS parquet AS
SELECT * FROM tim.occurrence_map_source 
TABLESAMPLE (BUCKET 1 OUT OF 1000 ON rand()) s;    
```

To create the HBase table use the following.
Notes:
- No MOB because we discovered https://issues.apache.org/jira/browse/HBASE-16841
- Data compresses well with Snappy (approx to 20%)
- Presplit so we have sensibly sized regions after creation 
- We salt and split, so there is no real benefit of data block encoding
- We have one field per key, and access addressing cell so bloom filtering is wasteful
```
disable 'dev_map'
drop 'dev_map'
create 'dev_map', 
  {NAME => 'EPSG_3857', VERSIONS => 1, COMPRESSION => 'SNAPPY', DATA_BLOCK_ENCODING => 'NONE', BLOOMFILTER => 'NONE'},
  {NAME => 'EPSG_4326', VERSIONS => 1, COMPRESSION => 'SNAPPY', DATA_BLOCK_ENCODING => 'NONE', BLOOMFILTER => 'NONE'},
  {NAME => 'EPSG_3575', VERSIONS => 1, COMPRESSION => 'SNAPPY', DATA_BLOCK_ENCODING => 'NONE', BLOOMFILTER => 'NONE'},
  {SPLITS => ['1','2','3','4','5','6','7','8','9']}

```

Then you can load the files using something like this:
```shell
for z in $(seq 0 16); do
    hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3857/z$z dev_map
    hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_4326/z$z dev_map
    hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3575/z$z dev_map
done

hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/points/EPSG_4326 dev_map
```
