```
CREATE TABLE tim.occurrence_map_source STORED AS parquet AS
SELECT 
  decimalLatitude, decimalLongitude,
  datasetKey, publishingOrgKey, countryCode, publishingCountry,
  kingdomKey, phylumKey, classKey, orderKey, familyKey, genusKey, speciesKey, taxonKey,
  year, basisOfRecord
FROM
  prod_b.occurrence_hdfs
WHERE
  decimalLatitude IS NOT NULL AND decimalLongitude IS NOT NULL AND hasGeospatialIssues=false;
  
CREATE TABLE tim.occurrence_map_source_sample STORED AS parquet AS
SELECT * FROM tim.occurrence_map_source 
TABLESAMPLE (BUCKET 1 OUT OF 1000 ON rand()) s;    
```
