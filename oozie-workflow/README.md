# Oozie workflow

Oozie workflow for backfilling the maps.

Example usage:

 - To build: ```mvn clean package```
 - Copy to HDFS: ```hdfs dfs -copyFromLocal target/maps-backfill-workflow-dev /```
 - To launch: ```oozie job --oozie http://c1n2.gbif.org:11000/oozie -config ./bin/dev.properties -run```
