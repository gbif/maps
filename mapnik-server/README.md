# Mapnik Server

A very simple PNG writer of vector tiles that renders using Mapnik.

# Building and running

For continuous integration with the Java modules, this project can be built using Maven:

```
mvn clean install
cd src/main/node
export PATH="$PWD/node/":$PATH
node server.js ../../../server.conf 3000
```

`npm`, `nvm` and `npm` tool can be used instead, if preferred.
