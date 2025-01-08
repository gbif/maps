-- This template requires 2 parameters:
--  1) the projection name; i.e. mercator, wgs84, arctic or %1$s
--  2) the hive DB; e.g. tim

-- Mount a table backed by parquet files on HDFS
-- (Requires clickhouse to be configured with an hdfs-site.xml and an HA NN named as ha-nn)
CREATE TABLE hdfs_%1$s
(
    x UInt32,
    y UInt32,
    datasetkey UUID,
    publishingorgkey UUID,
    publishingcountry FixedString(2),
    networkkey Array(UUID),
    countrycode FixedString(2),
    basisofrecord LowCardinality(String),
    kingdomkey UInt8,
    phylumkey UInt32,
    classkey UInt32,
    orderkey UInt32,
    familykey UInt32,
    genuskey UInt32,
    specieskey UInt32,
    taxonkey UInt32,
    year UInt16,
    occcount UInt64
) ENGINE = HDFS('hdfs://ha-nn/user/hive/warehouse/%2$s.db/map_clickhouse_%1$s/*.parquet', 'parquet');
