#!/usr/bin/env bash
set -e
set -o pipefail

ENV=$1
TOKEN=$2

echo "Resuming points and pyramid workflows for $ENV"

echo "Get latest maps-coord config profiles from GitHub"
curl -Ss -H "Authorization: token $TOKEN" -H 'Accept: application/vnd.github.v3.raw' -O -L https://api.github.com/repos/gbif/gbif-configuration/contents/maps-builder/$ENV/points.properties
curl -Ss -H "Authorization: token $TOKEN" -H 'Accept: application/vnd.github.v3.raw' -O -L https://api.github.com/repos/gbif/gbif-configuration/contents/maps-builder/$ENV/tiles.properties

P_OOZIE=$(grep '^oozie.url=' points.properties | cut -d= -f 2)
T_OOZIE=$(grep '^oozie.url=' tiles.properties | cut -d= -f 2)

# Gets the Oozie id of the current coordinator job if it exists
WID=$(oozie jobs -oozie $P_OOZIE -jobtype coordinator -filter name=MapBuild-Points | awk 'NR==3 {print $1}')
if [ -n "$WID" ]; then
  echo "Resuming current coordinator job" $WID
  sudo -u hdfs oozie job -oozie $P_OOZIE -resume $WID
fi

echo "Waiting 65 seconds, so both jobs don't start in the same minute and clash with snapshot names"
sleep 65

WID=$(oozie jobs -oozie $T_OOZIE -jobtype coordinator -filter name=MapBuild-Tiles | awk 'NR==3 {print $1}')
if [ -n "$WID" ]; then
  echo "Resuming current coordinator job" $WID
  sudo -u hdfs oozie job -oozie $T_OOZIE -resume $WID
fi
