## A protobufer tile server

To build: ```mvn clean package```

To run: ```java -jar target/vectortile-server-0.1-SNAPSHOT-shaded.jar server server.conf```

###URL structure###
 
An overriding goal of the URL structure is to be _as consistent as possible_ with the occurrence API.  

There are different kinds of maps:
  1. High performance "simple maps" which are preprocessed to pixel precision and provide a variety of styling and projection options, with filtering only by ```basisOfRecord``` and ```year```
  2. Ad hoc search maps, which are significantly slower in performance and resolution, but provide the means to filter data using the complete occurrence search API.  

####1. Examples for occurrence density maps####
Simple (note, hasGeospatialIssue is not explicitly declated, but is always true for simple maps)
  - /occurrence/density/{z}/{x}/{y}.mvt?taxonKey=212
  - /occurrence/density/{z}/{x}/{y}.mvt?taxonKey=212&year=1980,2014
  - /occurrence/density/{z}/{x}/{y}.mvt?taxonKey=212&year=1980,2014&basisOfRecord=OBSERVATION&basisOfRecord=HUMAN_OBSERVATION    

Ad hoc search
  - /occurrence/density/adhoc/{z}/{x}/{y}.mvt?taxonKey=212&hasGeospatialIssue=true
