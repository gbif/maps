/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.maps;

import org.gbif.maps.common.hbase.ModulusSalt;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.gbif.maps.udf.EncodeBorYearUDF;
import org.gbif.maps.udf.GlobalPixelUDF;
import org.gbif.maps.udf.HBaseKeyUDF;
import org.gbif.maps.udf.MapKeysUDF;
import org.gbif.maps.udf.TileXYUDF;

import scala.Tuple2;

/** Builds HFiles for the pyramid of map tiles. */
@Slf4j
@Builder
class TileMapBuilder implements Serializable {
  private final SparkSession spark;
  private final Set<String> largeMapKeys;
  private final ModulusSalt salter;
  private final String sourceTable;
  private final String targetDir;
  private final Configuration hadoopConf;
  private final int tileSize;
  private final int bufferSize;
  private final int maxZoom;
  private final int projectionParallelism;

  /** Generates the tile pyramid. */
  void generate() {
    // process an input table for the map keys
    String inputCacheTable = prepareInput(spark);

    runProjection(spark, "EPSG:3857", inputCacheTable);
    runProjection(spark, "EPSG:4326", inputCacheTable);
    runProjection(spark, "EPSG:3575", inputCacheTable);

    // Optimisation since there are fewer records in the southern hemisphere (100km buffer)
    String southCacheTable =
        filterToNewTable(
            spark, inputCacheTable, String.format("%s_south", inputCacheTable), "lat<=1");

    spark.sql(String.format("UNCACHE TABLE %s", inputCacheTable));

    runProjection(spark, "EPSG:3031", southCacheTable);
    spark.sql(String.format("UNCACHE TABLE %s", southCacheTable));
  }

  /** Runs the tile pyramid build for the projection */
  @SneakyThrows
  private void runProjection(SparkSession spark, String epsg, String table) {
    spark.sparkContext().setJobGroup(epsg, "Processing projection " + epsg, true);

    // Main projection function
    for (int z = maxZoom; z >= 0; z--) { // slowest first
      spark.sparkContext().setJobDescription("Processing zoom " + z);
      String dir = targetDir + "/tiles/" + epsg.replaceAll(":", "_") + "/z" + z;

      Dataset<Row> tileData = createTiles(spark, table, epsg, z);
      JavaPairRDD<String, byte[]> vectorTiles = generateMVTs(tileData);
      writeHFiles(vectorTiles, dir, epsg);
    }
  }

  /** Creates a new input table that includes the mapKeys for those views that require tiling. */
  private String prepareInput(SparkSession spark) {
    spark.sparkContext().setJobGroup("PREPARE", "Preparing input data", true);
    MapKeysUDF.register(spark, "mapKeys", largeMapKeys, true);
    EncodeBorYearUDF.register(spark, "encodeBorYear");

    String targetTable = String.format("%s_filtered", sourceTable);
    spark.sql(
        String.format(
            "CACHE TABLE %s AS "
                + "SELECT "
                + "  mapKey, "
                + "  decimalLatitude AS lat, "
                + "  decimalLongitude AS lng, "
                + "  encodeBorYear(basisOfRecord, year) AS borYear, " // improves performance
                + "  count(*) AS occCount "
                + "FROM "
                + "  %s "
                + "  LATERAL VIEW explode(  "
                + "    mapKeys("
                + "      kingdomKey, phylumKey, classKey, orderKey, familyKey, genusKey, speciesKey, taxonKey,"
                + "      datasetKey, publishingOrgKey, countryCode, publishingCountry, networkKey"
                + "    ) "
                + "  ) m AS mapKey "
                + "GROUP BY mapKey, lat, lng, borYear "
                + "ORDER BY mapKey, borYear",
            targetTable, sourceTable));
    return targetTable;
  }

  /**
   * Performs the aggregations of counts for each pixel in the tiles. This projects coordinates to
   * the global XY space, then aggregates counts at the pixel, then groups the pixels into tiles
   * noting that a pixel can fall on a tile and in a buffer of an adjacent tile.
   */
  private Dataset<Row> createTiles(SparkSession spark, String table, String epsg, int zoom) {

    // filter input and project to global pixel address
    GlobalPixelUDF.register(spark, "project", epsg, tileSize);
    Dataset<Row> t1 =
        spark.sql(
            String.format(
                "      SELECT "
                    + "  mapKey, "
                    + "  project(%d, lat, lng) AS xy, "
                    + "  struct(borYear AS borYear, sum(occCount) AS occCount) AS borYearCount "
                    + "FROM %s "
                    + "GROUP BY mapKey, xy, borYear",
                zoom, table));
    t1.createOrReplaceTempView("t1");

    // collect counts into a feature at the global pixel address
    Dataset<Row> t2 =
        spark.sql(
            "SELECT mapKey, xy, collect_list(borYearCount) as features"
                + "  FROM t1 "
                + "  WHERE xy IS NOT NULL"
                + "  GROUP BY mapKey, xy");
    t2.createOrReplaceTempView("t2");

    // readdress pixels onto tiles noting that addresses in buffer zones fall on multiple tiles
    HBaseKeyUDF.registerTileKey(spark, "hbaseKey", salter);
    TileXYUDF.register(spark, "collectToTiles", epsg, tileSize, bufferSize);
    return spark.sql(
        String.format(
            "SELECT "
                + "    hbaseKey(mapKey, %d, tile.tileX, tile.tileY) AS key,"
                + "    collect_list("
                + "      struct(tile.pixelX AS x, tile.pixelY AS y, features)"
                + "    ) AS tile "
                + "  FROM "
                + "    t2 "
                + "    LATERAL VIEW explode("
                + "      collectToTiles(%d, xy.x, xy.y)" // readdresses global pixels
                + "    ) t AS tile "
                + "  GROUP BY key",
            zoom, zoom));
  }

  /** Generates the Vector Tiles for the provided data. */
  private JavaPairRDD<String, byte[]> generateMVTs(Dataset<Row> source) {
    VectorTiles vectorTiles = new VectorTiles(tileSize, bufferSize);
    // UDF avoided as it proved slower due to conversion of the scala wrapper around byte[]
    return source
        .toJavaRDD()
        .mapToPair(
            (PairFunction<Row, String, byte[]>)
                r -> {
                  String saltedKey = r.getString(0);
                  List<Row> tileData = r.getList(1);
                  byte[] mvt = vectorTiles.generate(tileData);
                  return new Tuple2<>(saltedKey, mvt);
                });
  }

  /**
   * Generates the HFiles containing the tiles and saves them in the provided directory. This
   * partitions the data using the modulus of the prefix salt to match the target regions, sorts
   * within the partitions and then creates the HFiles.
   */
  private void writeHFiles(JavaPairRDD<String, byte[]> mvts, String dir, String epsg) {
    byte[] colFamily = Bytes.toBytes(epsg.replaceAll(":", "_"));
    byte[] col = Bytes.toBytes("tile");
    mvts.repartitionAndSortWithinPartitions(new SaltPrefixPartitioner(salter.saltCharCount()))
        .mapToPair(
            (PairFunction<Tuple2<String, byte[]>, ImmutableBytesWritable, KeyValue>)
                kvp -> {
                  byte[] saltedRowKey = Bytes.toBytes(kvp._1);
                  byte[] mvt = kvp._2;
                  ImmutableBytesWritable key = new ImmutableBytesWritable(saltedRowKey);
                  KeyValue row = new KeyValue(saltedRowKey, colFamily, col, mvt);
                  return new Tuple2<>(key, row);
                })
        .saveAsNewAPIHadoopFile(
            dir,
            ImmutableBytesWritable.class,
            KeyValue.class,
            HFileOutputFormat2.class,
            hadoopConf);
  }

  /** Creates a new table using the provided filter */
  private String filterToNewTable(SparkSession spark, String source, String target, String filter) {
    spark.sql(
        String.format(
            "CACHE TABLE %s AS " + "SELECT mapKey,lat,lng,borYear,occCount FROM %s WHERE %s",
            target, source, filter));
    return target;
  }
}
