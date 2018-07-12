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

# Gets the Oozie id of the current coordinator job if it exists
WID=$(oozie jobs -oozie $P_OOZIE -jobtype coordinator -filter name=MapBuild-Points | awk 'NR==3 {print $1}')
if [ -n "$WID" ]; then
  echo "Killing current coordinator job" $WID
  sudo -u hdfs oozie job -oozie $P_OOZIE -kill $WID
fi

WID=$(oozie jobs -oozie $T_OOZIE -jobtype coordinator -filter name=MapBuild-Tiles | awk 'NR==3 {print $1}')
if [ -n "$WID" ]; then
  echo "Killing current coordinator job" $WID
  sudo -u hdfs oozie job -oozie $T_OOZIE -kill $WID
fi

echo "Assembling jar for $ENV"
# Oozie uses timezone UTC
mvn -Dpoints.frequency="$P_FREQUENCY" -Dpoints.start="$P_START" -Dtiles.frequency="$T_FREQUENCY" -Dtiles.start="$T_START" -DskipTests -Duser.timezone=UTC clean install

echo "Copy to Hadoop"
sudo -u hdfs hdfs dfs -rm -r /maps-backfill-workflow/
sudo -u hdfs hdfs dfs -copyFromLocal target/maps-backfill-workflow /

echo "Start Oozie points job"
sudo -u hdfs oozie job --oozie $P_OOZIE -config points.properties -run
echo "Waiting 65 seconds, so both jobs don't start in the same minute and clash with snapshot names"
sleep 65
echo "Start Oozie tiles job"
sudo -u hdfs oozie job --oozie $T_OOZIE -config tiles.properties -run
