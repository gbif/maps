-- This template requires 1 parameter:
--  1) the projection name; i.e. mercator, wgs84, arctic or %1$s

INSERT INTO occurrence_%1$s
SELECT toUInt32(x), toUInt32(y), datasetkey, publishingorgkey,
  publishingcountry, networkkey, countrycode, basisofrecord, kingdomkey,
  phylumkey, classkey, orderkey, familykey, genuskey, specieskey, taxonkey,
  year, occcount
FROM hdfs_%1$s;
