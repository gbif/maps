# Mapnik Server

**This is experimental!**

This attempts to use an Express server running a tile-live convertion of the MVTs from the vector-tile server to PNGs rendered using Mapnik.

# Building and running

For continuous integration with the Java modules, this project can be built using Maven:

```
mvn clean install
cd src/main/node
export PATH="$PWD/node/":$PATH
node server.js ../../../server.conf 3000
```

`npm`, `nvm` and `npm` tool can be used instead, if preferred.
