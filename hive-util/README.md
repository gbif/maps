## Hive utilities relating to maps
 
Backfills are the process by which we batch process and build the HBase tables.

Here we are using Hive to create HFiles which are then bulk loaded into HBase.

```
use tim;
  
CREATE TABLE occurrence AS 
SELECT * FROM dev.occurrence_hdfs;

-- in Hive:
DROP TABLE coords_hbase; 




CREATE TABLE coords_hbase(id INT, x DOUBLE, y DOUBLE)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
  'hbase.columns.mapping' = ':key,o:x,o:y',
  'hbase.table.default.storage.type' = 'binary');

CREATE TABLE coords_hbase(id INT, x DOUBLE, y DOUBLE)
STORED BY 'org.gbif.hive.hbase.GBIFHBaseStorageHandler'
WITH SERDEPROPERTIES (
  'hbase.columns.mapping' = ':key,o:x,o:y',
  'hbase.table.default.storage.type' = 'binary');

-- note that the /o must match the column family in the SERDEPROPERTIES
-- SET hive.mapred.reduce.tasks.speculative.execution=false;
-- SET mapreduce.reduce.speculative=false;

SET hfile.family.path=/tmp/coords_hbase_gbifhfiles/o; 
SET hive.hbase.generatehfiles=true;

-- VERY IMPORTANT
SET user.name=yarn;
SET mapreduce.job.user.name=yarn;
SET hive.user.name=yarn;
SET hive.mapreduce.job.user.name=yarn;

INSERT OVERWRITE TABLE coords_hbase 
SELECT gbifId, decimalLongitude, decimalLatitude
FROM occurrence
WHERE gbifId IS NOT NULL AND gbifID>1
CLUSTER BY gbifId;




use tim;
add jar /tmp/hive-io-1.0-SNAPSHOT.jar;
DROP TABLE coords_hbase; 

CREATE TABLE coords_hbase(id INT, x DOUBLE, y DOUBLE)
STORED BY 'org.gbif.hive.hbase.GBIFHBaseStorageHandler'
WITH SERDEPROPERTIES (
  'hbase.columns.mapping' = ':key,o:x,o:y',
  'hbase.table.default.storage.type' = 'binary');

SET hfile.family.path=/tmp/gbif2/coords_hfiles/o; 
SET hive.hbase.generatehfiles=true;
-- set hive.exec.reducers.bytes.per.reducer=5368709000;

INSERT OVERWRITE TABLE coords_hbase 
SELECT gbifId, decimalLongitude, decimalLatitude 
FROM occurrence
WHERE gbifId IS NOT NULL AND gbifID>1
and decimalLatitude is not null and decimallongitude is not null
CLUSTER BY gbifId;

create table test AS 
SELECT gbifId, decimalLongitude, decimalLatitude
FROM occurrence
WHERE gbifId IS NOT NULL AND gbifID>1
and decimalLatitude is not null and decimallongitude is not null
CLUSTER BY gbifId;


hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dcreate.table=no /tmp/gbif2/coords_hfiles tim.coords_hbase 

``` 


-- in HBase:
create 't1', {NAME => 'o'}



CREATE TABLE coords_hbase(id INT, x DOUBLE, y DOUBLE)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
  'hbase.columns.mapping' = ':key,o:taxonKey,o:speciesKey',
  '' = '',
  'hbase.table.default.storage.type' = 'binary');



SET z=0;

SELECT 
  toKey(${hiveconf:z}, tileXY.x, tileXY.y) AS key,
  toMapboxVectorTile(tileLocalPixelXY.x, tileLocalPixelXY.y, year, count) AS vectorTile
FROM
  (SELECT
    toTileXY(lat,lng,${hiveconf:z}) AS tileXY,
    toTileLocalPixelXY(lat,lng,${hiveconf:z}) AS tileLocalPixelXY,
    year AS year,
    count(*) AS count
  FROM
    occurrence
  GROUP BY
    toTileXY(lat,lng,${hiveconf:z}) AS tileXY,
    toTileLocalPixelXY(lat,lng,z) AS tileLocalPixelXY,
    year AS year) t1
GROUP BY
  t1.tileXY.x, t1.tileXY.y;


USE tim;
ADD JAR /tmp/hive-util-0.1-SNAPSHOT.jar;
ADD JAR /tmp/common-0.1-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION toTileXY AS 'org.gbif.hive.udf.TileAddressUDF';
CREATE TEMPORARY FUNCTION tile AS 'org.gbif.hive.udf.TileUDF';

SET tileSize = 4096;
SET z = 0;

SELECT
  tileXY.x, tileXY.y, year, count(*) AS count
FROM (
  SELECT 
    toTileXY(${hiveconf:tileSize}, ${hiveconf:z}, decimalLatitude, decimalLongitude) AS tileXY,
    year AS year
  FROM 
    occurrence
  WHERE 
    decimalLatitude IS NOT NULL AND
    decimalLongitude IS NOT NULL    
) t1
GROUP BY tileXY.x, tileXY.y, year
ORDER BY tileXY.x, tileXY.y, year;
  
USE tim;
ADD JAR /tmp/hive-util-0.1-SNAPSHOT.jar;
ADD JAR /tmp/common-0.1-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION toTileXY AS 'org.gbif.hive.udf.TileAddressUDF';
CREATE TEMPORARY FUNCTION tile AS 'org.gbif.hive.udf.TileUDF';

SET tileSize = 4096;
SET z = 0;

SET mapreduce.map.java.opts=-Xmx1G;

SET mapreduce.reduce.shuffle.input.buffer.percent=0.2;
SET mapreduce.reduce.shuffle.parallelcopies=5;
SET mapreduce.map.memory.mb=4096;
SET mapreduce.map.java.opts=-Xmx3584m;
SET mapreduce.reduce.memory.mb=8192;
SET mapreduce.reduce.java.opts=-Xmx7680m;

CREATE TABLE tim.delme STORED AS orc AS
SELECT
  tile.x, tile.y, tile.px, tile.py, year, count(*) AS count
FROM (
  SELECT 
    tile(${hiveconf:tileSize}, ${hiveconf:z}, decimalLatitude, decimalLongitude) AS tile,
    year AS year
  FROM 
    occurrence
  WHERE 
    decimalLatitude IS NOT NULL AND
    decimalLongitude IS NOT NULL    
) t1
GROUP BY tile.x, tile.y, tile.px, tile.py, year
ORDER BY tile.x, tile.y, tile.px, tile.py, year;  
  
  
DROP TABLE tim.delme7;
CREATE TABLE tim.delme7 STORED AS orc AS 
SELECT x/2 as x, y/2 as y, px/2 as px, py/2 as py, year, sum(count) as count
FROM tim.delme
GROUP BY x/2, y/2, px/2, py/2, year; 

CREATE TABLE tim.delme6 STORED AS orc AS 
SELECT x/2 as x, y/2 as y, px/2 as px, py/2 as py, year, sum(count) as count
FROM tim.delme7
GROUP BY x/2, y/2, px/2, py/2, year; 

CREATE TABLE tim.delme5 STORED AS orc AS 
SELECT x/2 as x, y/2 as y, px/2 as px, py/2 as py, year, sum(count) as count
FROM tim.delme6
GROUP BY x/2, y/2, px/2, py/2, year; 

CREATE TABLE tim.delme4 STORED AS orc AS 
SELECT x/2 as x, y/2 as y, px/2 as px, py/2 as py, year, sum(count) as count
FROM tim.delme5
GROUP BY x/2, y/2, px/2, py/2, year; 

CREATE TABLE tim.delme3 STORED AS orc AS 
SELECT x/2 as x, y/2 as y, px/2 as px, py/2 as py, year, sum(count) as count
FROM tim.delme4
GROUP BY x/2, y/2, px/2, py/2, year; 

CREATE TABLE tim.delme2 STORED AS orc AS 
SELECT x/2 as x, y/2 as y, px/2 as px, py/2 as py, year, sum(count) as count
FROM tim.delme3
GROUP BY x/2, y/2, px/2, py/2, year; 

CREATE TABLE tim.delme1 STORED AS orc AS 
SELECT x/2 as x, y/2 as y, px/2 as px, py/2 as py, year, sum(count) as count
FROM tim.delme2
GROUP BY x/2, y/2, px/2, py/2, year; 

CREATE TABLE tim.delme0 STORED AS orc AS 
SELECT x/2 as x, y/2 as y, px/2 as px, py/2 as py, year, sum(count) as count
FROM tim.delme1
GROUP BY x/2, y/2, px/2, py/2, year; 

  
