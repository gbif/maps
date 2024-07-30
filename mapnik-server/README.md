# Mapnik Server

A PNG writer of vector tiles that renders using Mapnik.

## Building and running

For continuous integration with the Java modules, this project is built using Maven:

```
mvn clean install
docker run --rm -it --volume $PWD/conf:/usr/local/gbif/conf --publish 8080:8080 docker.gbif.org/mapnik-server:dev
```

## Backward compatibility with V1 API

The server answers requests conforming to the V1 GBIF mapping API, including serving the HTML interface.  This
interface is from the `tile-server` project. If changes are needed, make them there and copy the files over.
