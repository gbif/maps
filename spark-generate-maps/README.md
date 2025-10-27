Generates the tile tables in HBase for both point and tile pyramid maps.

```
disable 'dev_map'
drop 'dev_map'
create 'dev_map',
  {NAME => 'EPSG_3857', VERSIONS => 1, COMPRESSION => 'SNAPPY', DATA_BLOCK_ENCODING => 'FAST_DIFF', BLOOMFILTER => 'NONE'},
  {NAME => 'EPSG_4326', VERSIONS => 1, COMPRESSION => 'SNAPPY', DATA_BLOCK_ENCODING => 'FAST_DIFF', BLOOMFILTER => 'NONE'},
  {NAME => 'EPSG_3575', VERSIONS => 1, COMPRESSION => 'SNAPPY', DATA_BLOCK_ENCODING => 'FAST_DIFF', BLOOMFILTER => 'NONE'},
  {NAME => 'EPSG_3031', VERSIONS => 1, COMPRESSION => 'SNAPPY', DATA_BLOCK_ENCODING => 'FAST_DIFF', BLOOMFILTER => 'NONE'},
  {NUMREGIONS => 10, SPLITALGO => 'DecimalStringSplit'}

disable 'test_map'
drop 'test_map'
create 'test_map',
  {NAME => 'EPSG_3857', VERSIONS => 1, COMPRESSION => 'SNAPPY', DATA_BLOCK_ENCODING => 'FAST_DIFF', BLOOMFILTER => 'NONE'},
  {NAME => 'EPSG_4326', VERSIONS => 1, COMPRESSION => 'SNAPPY', DATA_BLOCK_ENCODING => 'FAST_DIFF', BLOOMFILTER => 'NONE'},
  {NAME => 'EPSG_3575', VERSIONS => 1, COMPRESSION => 'SNAPPY', DATA_BLOCK_ENCODING => 'FAST_DIFF', BLOOMFILTER => 'NONE'},
  {NAME => 'EPSG_3031', VERSIONS => 1, COMPRESSION => 'SNAPPY', DATA_BLOCK_ENCODING => 'FAST_DIFF', BLOOMFILTER => 'NONE'},
  {NUMREGIONS => 100, SPLITALGO => 'DecimalStringSplit'}

```

Then you can load the files using something like this:
```shell
for z in $(seq 0 16); do
    sudo -u hdfs hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3857/z$z dev_map
    sudo -u hdfs hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_4326/z$z dev_map
    sudo -u hdfs hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3575/z$z dev_map
    sudo -u hdfs hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/tiles/EPSG_3031/z$z dev_map
done

sudo -u hdfs hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=1000 -Dcreate.table=no /tmp/tim_maps/points/EPSG_4326 dev_map
```
