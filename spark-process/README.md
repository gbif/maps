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
```
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3857/z0 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3857/z1 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3857/z2 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3857/z3 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3857/z4 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3857/z5 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3857/z6 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3857/z7 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3857/z8 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3857/z9 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3857/z10 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3857/z11 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3857/z12 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3857/z13 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3857/z14 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3857/z15 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3857/z16 tim_test

hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_4326/z0 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_4326/z1 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_4326/z2 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_4326/z3 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_4326/z4 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_4326/z5 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_4326/z6 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_4326/z7 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_4326/z8 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_4326/z9 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_4326/z10 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_4326/z11 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_4326/z12 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_4326/z13 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_4326/z14 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_4326/z15 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_4326/z16 tim_test

    
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3575/z0 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3575/z1 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3575/z2 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3575/z3 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3575/z4 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3575/z5 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3575/z6 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3575/z7 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3575/z8 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3575/z9 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3575/z10 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3575/z11 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3575/z12 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3575/z13 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3575/z14 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3575/z15 tim_test
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3575/z16 tim_test
    
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/points/EPSG_4326 tim_test

```
