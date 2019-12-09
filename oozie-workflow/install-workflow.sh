#!/usr/bin/env bash
set -e
set -o pipefail

ENV=$1
TOKEN=$2

echo "Installing points and pyramid workflows for $ENV"

echo "Get latest maps-coord config profiles from GitHub"
curl -Ss -H "Authorization: token $TOKEN" -H 'Accept: application/vnd.github.v3.raw' -O -L https://api.github.com/repos/gbif/gbif-configuration/contents/maps-builder/$ENV/points.properties
curl -Ss -H "Authorization: token $TOKEN" -H 'Accept: application/vnd.github.v3.raw' -O -L https://api.github.com/repos/gbif/gbif-configuration/contents/maps-builder/$ENV/tiles.properties

P_START=$(date +%Y-%m-%d)T$(grep '^startHour=' points.properties | cut -d= -f 2)Z
P_FREQUENCY="$(grep '^frequency=' points.properties | cut -d= -f 2)"
P_OOZIE=$(grep '^oozie.url=' points.properties | cut -d= -f 2)

T_START=$(date +%Y-%m-%d)T$(grep '^startHour=' tiles.properties | cut -d= -f 2)Z
T_FREQUENCY="$(grep '^frequency=' tiles.properties | cut -d= -f 2)"
T_OOZIE=$(grep '^oozie.url=' tiles.properties | cut -d= -f 2)


echo "Assembling jar for $ENV"
# Oozie uses timezone UTC
mvn -Dpoints.frequency="$P_FREQUENCY" -Dpoints.start="$P_START" -Dtiles.frequency="$T_FREQUENCY" -Dtiles.start="$T_START" -DskipTests -Duser.timezone=UTC clean install

echo "Copy to Hadoop"
sudo -u hdfs hdfs dfs -rm -r /mapsavro-backfill-workflow/
sudo -u hdfs hdfs dfs -mkdir /mapsavro-backfill-workflow/
sudo -u hdfs hdfs dfs -copyFromLocal target/maps-backfill-workflow/* /mapsavro-backfill-workflow/
sudo -u hdfs hdfs dfs -copyFromLocal /etc/hive/conf/hive-site.xml /mapsavro-backfill-workflow/lib/

sudo -u hdfs oozie job --oozie $T_OOZIE -config tiles.properties -run
