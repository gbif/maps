#!/bin/bash
USER=$(whoami)
SOURCE="./target/spark-generate-0.37.0-SNAPSHOT.jar"
POD="${2:-gbif-hdfs-namenode-default-0}"
NAME_SPACE="${3:-gbif-develop}"

if [ ! -x $SOURCE ];
then
    FILE_NAME=$(basename $SOURCE)
    echo "Transfering file ${FILE_NAME} to ${POD} in namespace: ${NAME_SPACE}."
    echo "Creating tmp dir for file."
    kubectl -n "${NAME_SPACE}" exec "pod/${POD}" -c namenode -- /bin/sh -c "mkdir -p /tmp/${USER}" && \
    echo "Copying file to tmp dir." && \
    kubectl cp "${SOURCE}" "${NAME_SPACE}/${POD}:/tmp/${USER}" -c namenode && \
    echo "Adding file: ${FILE_NAME} to HDFS in /tmp folder." && \
    kubectl -n "${NAME_SPACE}" exec "pod/${POD}" -c namenode -- /bin/sh -c "./bin/hdfs dfs -rm /tmp/${FILE_NAME}" && \
    kubectl -n "${NAME_SPACE}" exec "pod/${POD}" -c namenode -- /bin/sh -c "./bin/hdfs dfs -put /tmp/${USER}/${FILE_NAME} /tmp" && \
    echo "Removing tmp dir." && \
    kubectl -n "${NAME_SPACE}" exec "pod/${POD}" -c namenode -- /bin/sh -c "rm -rf /tmp/${USER}"
else
    echo "You need to provide a file to transfer to HDFS."
fi
# 2.0.6 for slf libs
