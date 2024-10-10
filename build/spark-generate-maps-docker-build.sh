#Simple script for pushing a image containing the named modules build artifact
MODULE="spark-generate-maps"

POM_VERSION=$(mvn -q -Dexec.executable="echo" -Dexec.args='${project.version}' --non-recursive exec:exec)

echo "Building Docker image: docker.gbif.org/${MODULE}:${POM_VERSION}"
docker build -f ./${MODULE}/docker/Dockerfile ./${MODULE} --build-arg JAR_FILE=${MODULE}-${POM_VERSION}.jar -t docker.gbif.org/${MODULE}:${POM_VERSION}

echo "Pushing Docker image to the repository"
docker push docker.gbif.org/${MODULE}:${POM_VERSION}

if [[ $IS_M2RELEASEBUILD = true ]]; then
  echo "Updated latest tag poiting to the newly released docker.gbif.org/${MODULE}:${POM_VERSION}"
  docker tag docker.gbif.org/${MODULE}:${POM_VERSION} docker.gbif.org/${MODULE}:latest
  docker push docker.gbif.org/${MODULE}:latest
fi

echo "Removing local Docker image: docker.gbif.org/${MODULE}:${POM_VERSION}"
docker rmi docker.gbif.org/${MODULE}:${POM_VERSION}

if [[ $IS_M2RELEASEBUILD = true ]]; then
  echo "Removing local Docker image with latest tag: docker.gbif.org/${MODULE}:latest"
  docker rmi docker.gbif.org/${MODULE}:latest
fi
