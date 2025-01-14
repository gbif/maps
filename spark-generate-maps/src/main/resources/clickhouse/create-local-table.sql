-- This template requires 1 parameter:
--  1) the projection name; i.e. mercator, wgs84, arctic or %1$s
CREATE TABLE %1$s.occurrence_%2$s
(
    x UInt32,
    y UInt32,
    INDEX idx_x (x) TYPE minmax,
    INDEX idx_y (y) TYPE minmax,
    datasetKey UUID,
    publishingOrgKey UUID,
    publishingCountry FixedString(2),
    networkKey Array(UUID),
    countryCode FixedString(2),
    basisOfRecord LowCardinality(String),
    kingdomKey LowCardinality(String),
    phylumKey String,
    classKey String,
    orderKey String,
    familyKey String,
    genusKey String,
    speciesKey String,
    taxonKey String,
    year UInt16,
    occCount UInt64
) ENGINE = MergeTree ORDER BY (mortonEncode(x, y));
