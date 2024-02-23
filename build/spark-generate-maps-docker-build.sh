#Simple script for pushing a image containing the named modules build artifact
MODULE="spark-generate-maps"
docker build -f ./${MODULE}/docker/Dockerfile ./${MODULE} --build-arg JAR_FILE=${MODULE}-${POM_VERSION}.jar -t docker.gbif.org/${MODULE}:${POM_VERSION}
docker push docker.gbif.org/${MODULE}:${POM_VERSION}
if [[ $IS_M2RELEASEBUILD = true ]]; then
  echo "Updated latest tag poiting to the newly released docker.gbif.org/${MODULE}:${POM_VERSION}"
  docker tag docker.gbif.org/${MODULE}:${POM_VERSION} docker.gbif.org/${MODULE}:latest
  docker push docker.gbif.org/${MODULE}:latest
fi
