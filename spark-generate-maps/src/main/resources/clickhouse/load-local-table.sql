-- This template requires 1 parameter:
--  1) the projection name; i.e. mercator, wgs84, arctic or %1$s

INSERT INTO %1$s.occurrence_%2$s
SELECT toUInt32(x), toUInt32(y), datasetKey, publishingOrgKey,
  publishingCountry, networkKey, countryCode, basisOfRecord, kingdomKey,
  phylumKey, classKey, orderKey, familyKey, genusKey, speciesKey, taxonKey,
  year, occCount
FROM %1$s.hdfs_%2$s;
