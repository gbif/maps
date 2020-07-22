# A Vector Tile Server

Build and run:

To build this a Maven profile with the following settings is required:
```
 <profile>
      <id>vectortile</id>
      <properties>
        <hbase.zookeeperQuorum>HBaseZookeeper</hbase.zookeeperQuorum>
        <hbase.tilesTableName>maps_tiles_table</hbase.tilesTableName>
        <hbase.pointsTableName>maps_points_table</hbase.pointsTableName>
        <!-- Following are default  values-->
        <hbase.tileSize>512</hbase.tileSize>
        <hbase.bufferSize>64</hbase.bufferSize>
        <hbase.saltModulus>100</hbase.saltModulus>

        <metastore.zookeeperQuorum>metaStoreZookeper</metastore.zookeeperQuorum>
        <metastore.path>metaStorePath</metastore.path>

        <esConfiguration.elasticsearch.hosts>elasticsearch_hosts</esConfiguration.elasticsearch.hosts>
        <esConfiguration.elasticsearch.index>occurrence_index_name</esConfiguration.elasticsearch.index>
        <!-- Following are default  values-->
        <esConfiguration.tileSize>512</esConfiguration.tileSize>
        <esConfiguration.bufferSize>64</esConfiguration.bufferSize>
      </properties>
    </profile>
```

```
mvn clean package -U -Pvectortile
java -jar target/vectortile-server-[0-9]*[0-9]-SNAPSHOT.jar
```

### URL structure

An overriding goal of the URL structure is to be _as consistent as possible_ with the occurrence API.

There are different kinds of maps:
  1. High performance "simple maps" which are preprocessed to pixel precision and provide a variety of styling and projection options, with filtering only by ```basisOfRecord``` and ```year```
  2. Ad hoc search maps, which are significantly slower in performance and resolution, but provide the means to filter data using the complete occurrence search API.

#### Examples for occurrence density maps
Simple (note, hasGeospatialIssue is not explicitly declared, but is always false for simple maps)
  - /map/occurrence/density/{z}/{x}/{y}.mvt?taxonKey=212
  - /map/occurrence/density/{z}/{x}/{y}.mvt?taxonKey=212&year=1980,2014
  - /map/occurrence/density/{z}/{x}/{y}.mvt?taxonKey=212&year=1980,2014&basisOfRecord=OBSERVATION&basisOfRecord=HUMAN_OBSERVATION
  - /map/occurrence/density/{z}/{x}/{y}.mvt?taxonKey=212&year=1980,2014&basisOfRecord=OBSERVATION&basisOfRecord=HUMAN_OBSERVATION&country=US

Ad hoc search
  - /map/occurrence/density/adhoc/{z}/{x}/{y}.mvt?taxonKey=212&hasGeospatialIssue=true
