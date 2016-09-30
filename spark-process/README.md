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

To create the HBase table:
```
disable 'tim_test'
drop 'tim_test'
create 'tim_test', 
  {NAME => 'EPSG_3857', VERSIONS => 1, COMPRESSION => 'SNAPPY', IS_MOB => true},
  {NAME => 'EPSG_4326', VERSIONS => 1, COMPRESSION => 'SNAPPY', IS_MOB => true},
  {NAME => 'EPSG_3575', VERSIONS => 1, COMPRESSION => 'SNAPPY', IS_MOB => true}
```

Then you can load the files using something like this:
```shell
for z in $(seq 0 16); do
    hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3857/z$z tim_test
    hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_4326/z$z tim_test
    hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3575/z$z tim_test
done

hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/points/EPSG_4326 tim_test
```
