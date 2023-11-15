FROM docker.gbif.org/gbif/base-image:1.0.0
ARG JAR_FILE=${JAVA_JAR_FILE}
USER root
RUN mkdir /jobs
COPY --chown=1000:1000 "$JAR_FILE" /jobs/spark-generate-maps.jar
USER gbif
