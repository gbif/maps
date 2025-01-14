-- This template requires 2 parameters:
--  1) the projection name; i.e. mercator, wgs84, arctic or %1$s
--  2) the hive DB; e.g. tim

-- Mount a table backed by parquet files on HDFS
-- (Requires clickhouse to be configured with an hdfs-site.xml and an HA NN named as ha-nn)
CREATE TABLE %1$s.hdfs_%2$s
(
    x UInt32,
    y UInt32,
    datasetKey UUID,
    publishingOrgKey UUID,
    publishingCountry FixedString(2),
    networkKey Array(UUID),
    countryCode FixedString(2),
    basisOfRecord LowCardinality(String),
    kingdomKey UInt8,
    phylumKey UInt32,
    classKey UInt32,
    orderKey UInt32,
    familyKey UInt32,
    genusKey UInt32,
    speciesKey UInt32,
    taxonKey UInt32,
    year UInt16,
    occCount UInt64
) ENGINE = HDFS('hdfs://gbif-hdfs/dev2/map_clickhouse_%2$s/*.parquet', 'parquet');
