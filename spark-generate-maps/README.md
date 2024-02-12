Generates the tile tables in HBase for both point and tile pyramid maps.

```
disable 'dev_map'
drop 'dev_map'
create 'dev_map',
  {NAME => 'EPSG_3857', VERSIONS => 1, COMPRESSION => 'SNAPPY', DATA_BLOCK_ENCODING => 'FAST_DIFF', BLOOMFILTER => 'NONE'},
  {NAME => 'EPSG_4326', VERSIONS => 1, COMPRESSION => 'SNAPPY', DATA_BLOCK_ENCODING => 'FAST_DIFF', BLOOMFILTER => 'NONE'},
  {NAME => 'EPSG_3575', VERSIONS => 1, COMPRESSION => 'SNAPPY', DATA_BLOCK_ENCODING => 'FAST_DIFF', BLOOMFILTER => 'NONE'},
  {SPLITS => ['1','2','3','4','5','6','7','8','9']}

disable 'uat_map'
drop 'uat_map'
create 'uat_map',
  {NAME => 'EPSG_3857', VERSIONS => 1, COMPRESSION => 'SNAPPY', DATA_BLOCK_ENCODING => 'FAST_DIFF', BLOOMFILTER => 'NONE'},
  {NAME => 'EPSG_4326', VERSIONS => 1, COMPRESSION => 'SNAPPY', DATA_BLOCK_ENCODING => 'FAST_DIFF', BLOOMFILTER => 'NONE'},
  {NAME => 'EPSG_3575', VERSIONS => 1, COMPRESSION => 'SNAPPY', DATA_BLOCK_ENCODING => 'FAST_DIFF', BLOOMFILTER => 'NONE'},
  {NAME => 'EPSG_3031', VERSIONS => 1, COMPRESSION => 'SNAPPY', DATA_BLOCK_ENCODING => 'FAST_DIFF', BLOOMFILTER => 'NONE'},
  {SPLITS => [
    '01','02','03','04','05','06','07','08','09','10',
    '11','12','13','14','15','16','17','18','19','20',
    '21','22','23','24','25','26','27','28','29','30',
    '31','32','33','34','35','36','37','38','39','40',
    '41','42','43','44','45','46','47','48','49','50',
    '51','52','53','54','55','56','57','58','59','60',
    '61','62','63','64','65','66','67','68','69','70',
    '71','72','73','74','75','76','77','78','79','80',
    '81','82','83','84','85','86','87','88','89','90',
    '91','92','93','94','95','96','97','98','99'
  ]}

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
