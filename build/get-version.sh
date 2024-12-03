#!/bin/bash -e

IS_M2RELEASEBUILD=$1

POM_VERSION=$(mvn -q -Dexec.executable="echo" -Dexec.args='${project.version}' --non-recursive exec:exec)

if [[ $IS_M2RELEASEBUILD = true ]]; then
  POM_VERSION=${POM_VERSION%-SNAPSHOT}
fi

echo "${POM_VERSION}"
