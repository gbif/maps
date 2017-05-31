# A Vector Tile Server

Build and run:

```
mvn clean compile exec:java
```

Or:

```
mvn clean package
java -jar target/vectortile-server-[0-9]*[0-9]-SNAPSHOT.jar server server.conf
```

### URL structure

An overriding goal of the URL structure is to be _as consistent as possible_ with the occurrence API.

There are different kinds of maps:
  1. High performance "simple maps" which are preprocessed to pixel precision and provide a variety of styling and projection options, with filtering only by ```basisOfRecord``` and ```year```
  2. Ad hoc search maps, which are significantly slower in performance and resolution, but provide the means to filter data using the complete occurrence search API.

#### Examples for occurrence density maps
Simple (note, hasGeospatialIssue is not explicitly declared, but is always true for simple maps)
  - /map/occurrence/density/{z}/{x}/{y}.mvt?taxonKey=212
  - /map/occurrence/density/{z}/{x}/{y}.mvt?taxonKey=212&year=1980,2014
  - /map/occurrence/density/{z}/{x}/{y}.mvt?taxonKey=212&year=1980,2014&basisOfRecord=OBSERVATION&basisOfRecord=HUMAN_OBSERVATION

Ad hoc search
  - /map/occurrence/density/adhoc/{z}/{x}/{y}.mvt?taxonKey=212&hasGeospatialIssue=true
