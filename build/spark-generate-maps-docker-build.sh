#Simple script for pushing a image containing the named modules build artifact
MODULE="spark-generate-maps"

POM_VERSION=$(mvn -q -Dexec.executable="echo" -Dexec.args='${project.version}' --non-recursive exec:exec)
IMAGE=docker.gbif.org/${MODULE}:${POM_VERSION}

echo "Building Docker image: ${IMAGE}"
docker build -f ./${MODULE}/docker/Dockerfile ./${MODULE} --build-arg JAR_FILE=${MODULE}-${POM_VERSION}.jar -t ${IMAGE}

echo "Pushing Docker image to the repository"
docker push ${IMAGE}

if [[ $IS_M2RELEASEBUILD = true ]]; then
  echo "Updated latest tag poiting to the newly released ${IMAGE}"
  docker tag ${IMAGE} docker.gbif.org/${MODULE}:latest
  docker push docker.gbif.org/${MODULE}:latest
fi

echo "Removing local Docker image: ${IMAGE}"
docker rmi ${IMAGE}

if [[ $IS_M2RELEASEBUILD = true ]]; then
  echo "Removing local Docker image with latest tag: docker.gbif.org/${MODULE}:latest"
  docker rmi docker.gbif.org/${MODULE}:latest
fi
