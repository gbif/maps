-- This template requires 1 parameter:
--  1) the projection name; i.e. mercator, wgs84, arctic or %1$s
CREATE TABLE occurrence_%1$s
(
    x UInt32,
    y UInt32,
    INDEX idx_x (x) TYPE minmax,
    INDEX idx_y (y) TYPE minmax,
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
) ENGINE = MergeTree ORDER BY (mortonEncode(x, y));
