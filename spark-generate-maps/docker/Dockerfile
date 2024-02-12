# Simple image that copy the already previous builds shaded jar into a specific location.
# This is used for SparkApplications to have a simple way to access the artifacts during their execution
FROM docker.gbif.org/gbif/base-image:1.0.0
ARG JAR_FILE=${JAVA_JAR_FILE}
USER root
RUN mkdir /jobs
COPY --chown=1000:1000 "./target/${JAR_FILE}" "/jobs/${JAR_FILE}"
USER gbif
