## Mercator

```
hive
ADD JAR /tmp/hive-udf-0.38.5-SNAPSHOT-jar-with-dependencies.jar;
CREATE TEMPORARY FUNCTION project AS 'org.gbif.maps.udf.ProjectUDF';

SET mapreduce.reduce.memory.mb=8192;
SET yarn.nodemanager.resource.memory-mb=12288;

DROP TABLE IF EXISTS tim.occurrence_mercator;
CREATE TABLE tim.occurrence_mercator STORED AS parquet AS
SELECT
 xy.x AS x, xy.y AS y,
 datasetKey, publishingOrgKey, publishingCountry, networkKey,
 countryCode, basisOfRecord, kingdomKey, phylumKey, classKey, orderKey, familyKey,
 genusKey, speciesKey, taxonKey, year, count(*) AS occCount
FROM (
  SELECT
    project(decimalLatitude, decimalLongitude, 'EPSG:3857', 16) AS xy,
    datasetKey, publishingOrgKey, publishingCountry, networkKey,
    countryCode, basisOfRecord, kingdomKey, phylumKey, classKey, orderKey, familyKey,
    genusKey, speciesKey, taxonKey, year
  FROM
    prod_h.occurrence
  WHERE
    occurrenceStatus = 'PRESENT'
    AND hasGeospatialIssues = false
    AND decimalLatitude BETWEEN -85 AND 85
) t
GROUP BY
  xy.x, xy.y, datasetKey, publishingOrgKey, publishingCountry, networkKey,
  countryCode, basisOfRecord, kingdomKey, phylumKey, classKey, orderKey, familyKey,
  genusKey, speciesKey, taxonKey, year;
```

With this created in Hive, we copy to the clickhouse server and create a clickhouse table.

Copy:

```
sudo rm -fr /var/lib/clickhouse/user_files/occurrence_mercator
sudo mkdir /var/lib/clickhouse/user_files/occurrence_mercator
sudo rclone sync c5:/user/hive/warehouse/tim.db/occurrence_mercator /var/lib/clickhouse/user_files/occurrence_mercator/.
```

Create and load table:

```
DROP TABLE IF EXISTS occurrence_mercator;
SET allow_suspicious_low_cardinality_types=1;
CREATE TABLE occurrence_mercator
(
    x UInt32,
    y UInt32,
    INDEX idx_x (x) TYPE minmax,
    INDEX idx_y (y) TYPE minmax,
    datasetkey LowCardinality(UUID),
    publishingorgkey LowCardinality(UUID),
    publishingcountry FixedString(2),
    networkkey Array(LowCardinality(UUID)),
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
    year LowCardinality(UInt16),
    occcount UInt64
) ENGINE = MergeTree ORDER BY (mortonEncode(x, y));

INSERT INTO occurrence_mercator
SELECT toUInt32(x), toUInt32(y), datasetkey, publishingorgkey,
  publishingcountry, networkkey, countrycode, basisofrecord, kingdomkey,
  phylumkey, classkey, orderkey, familykey, genuskey, specieskey, taxonkey,
  year, occcount
FROM file('occurrence_mercator/*', Parquet);
```

Add a user (not suitable for production use):

```
CREATE USER IF NOT EXISTS tim IDENTIFIED WITH no_password
SETTINGS
    add_http_cors_header = 1,
    max_result_rows = 1048576,
    enable_http_compression = 1,
    http_zlib_compression_level = 6,
    replace_running_query = 1,
    skip_unavailable_shards = 1,
    use_query_cache = 1,
    query_cache_ttl = 8640000,
    query_cache_share_between_users = 1,
    analyze_index_with_space_filling_curves = 0,
    max_execution_time = 180,
    priority CHANGEABLE_IN_READONLY,
    readonly = 1;
GRANT SELECT ON default.occurrence_mercator TO tim;
```
