# Mapnik Server

A PNG writer of vector tiles that renders using Mapnik.

## Building and running

For continuous integration with the Java modules, this project can be built using Maven:

```
mvn clean install
cd src/main/node
export PATH="$PWD/node/":$PATH
node server.js ../../../server.conf 3000
```

`npm` and `nvm` can be used instead, if preferred.

## Backward compatibility with V1 API

The server answers requests conforming to the V1 GBIF mapping API, including serving the HTML interface.  This
interface is from the `tile-server` project. If changes are needed, make them there and copy the files over.
