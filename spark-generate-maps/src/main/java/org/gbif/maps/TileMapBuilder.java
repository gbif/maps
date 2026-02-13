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
import org.gbif.maps.udf.*;

import java.io.Serializable;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import scala.Tuple2;

/** Builds HFiles for the pyramid of map tiles. */
@Slf4j
@Builder
class TileMapBuilder implements Serializable {
  private final SparkSession spark;
  private final TreeSet<String> largeMapKeys;
  private final ModulusSalt salter;
  private final String sourceTable;
  private final String targetDir;
  private final Configuration hadoopConf;
  private final int tileSize;
  private final int bufferSize;
  private final int maxZoom;
  private boolean nonTaxa; // process non Taxa views or not
  private LinkedHashMap<String, String> checklists; // alias : uuid

  /** Generates the tile pyramid. */
  void generate() {
    checklists = checklists == null ? new LinkedHashMap<>() : checklists; // defensive

    // To help optimise wide spark shuffles, dictionary encode the keys of interest to INT
    final Map<String, Integer> dictionary = dictionaryEncode(largeMapKeys);
    final Map<Integer, String> invertedDictionary = invert(dictionary); // required to decode

    MapKeysUDF.registerNonTaxonMapKeysUDF(spark, "nonTaxonMapKeys", dictionary);
    MapKeysUDF.registerTaxonKeysUDF(spark, "taxonMapKeys", dictionary);
    HBaseKeyUDF.registerTileKey(spark, "hbaseKey", salter, invertedDictionary);
    EncodeBorYearUDF.register(spark, "encodeBorYear");
    GlobalPixelUDF.register(spark, "project", tileSize);
    TileXYUDF.register(spark, "collectToTiles", tileSize, bufferSize);

    runProjection("EPSG:3857", null);
    runProjection("EPSG:4326", null);
    runProjection("EPSG:3575", "decimalLatitude>-1"); // 100km buffer
    runProjection("EPSG:3031", "decimalLatitude<1");
  }

  /**
   * Runs the process to generate the input tables and group into vector tiles written as HFiles.
   */
  private void runProjection(String epsg, String filterSQL) {

    String code = epsg.split(":")[1];

    if (nonTaxa) {
      String nonTaxonTable = "nonTaxon_" + code; // table suffix
      setDescription(epsg, "Preparing input for non-taxononomic keys");
      prepareInputTable(
          epsg,
          nonTaxonTable,
          "nonTaxonMapKeys(datasetKey, publishingOrgKey, countryCode, publishingCountry, networkKey)",
          filterSQL);
      for (int z = maxZoom; z >= 0; z--) {

        // Non-taxonomic maps
        setDescription(epsg, String.format("z[%d] non-taxonomic", z));
        String nonTaxonDir = String.format("%s/EPSG_%s/nonTaxon/z%d", targetDir, code, z);
        generateTiles(epsg, z, nonTaxonTable, nonTaxonDir);
      }

      spark.sql(String.format("DROP TABLE IF EXISTS %s PURGE", sourceTable + "_" + nonTaxonTable));
    }

    // Prepare checklist input
    for (Map.Entry<String, String> e : checklists.entrySet()) {
      String alias = e.getKey();
      String checklistKey = e.getValue();
      String checklistTable = "checklist_" + alias + "_" + code; // table suffix

      setDescription(epsg, String.format("Preparing input for taxonomy[%s]", alias));
      prepareInputTable(
          epsg,
          checklistTable,
          String.format("taxonMapKeys(classifications['%s'], '%s')", checklistKey, checklistKey),
          filterSQL);

      for (int z = maxZoom; z >= 0; z--) {
        setDescription(epsg, String.format("z[%d] taxonomy[%s]", z, alias));
        String checklistDir =
            String.format("%s/EPSG_%s/checklist-%s/z%d", targetDir, code, alias, z);
        generateTiles(epsg, z, checklistTable, checklistDir);
      }

      spark.sql(String.format("DROP TABLE IF EXISTS %s PURGE", sourceTable + "_" + checklistTable));
    }
  }

  /**
   * Optionally filters, and then projects the coordinates calculated for all zooms with accumulated
   * counts.
   */
  private void prepareInputTable(String epsg, String suffix, String mapKeysSQL, String where) {
    String whereSQL = where == null ? "" : " WHERE " + where + " ";

    // bit-shift to efficiently calculate pixel addresses at all zooms
    List<String> zoomCols = new ArrayList<>();
    for (int z = maxZoom; z >= 0; z--) {
      int shift = maxZoom - z;
      zoomCols.add(
          String.format(
              "struct(shiftRight(xy.x, %d) AS x, shiftRight(xy.y, %d) AS y) AS xy%d",
              shift, shift, z));
    }
    String zoomColumns = String.join(",\n  ", zoomCols);

    String sql =
        String.format(
            "      WITH global AS ("
                + "  SELECT "
                + "    mapKey, "
                + "    project('%s', %d, decimalLatitude, decimalLongitude) AS xy, "
                + "    encodeBorYear(basisOfRecord, year) AS borYear,"
                + "    count(*) as occCount "
                + "  FROM %s "
                + "    LATERAL VIEW explode(%s) m AS mapKey "
                + "  %s " // optional WHERE
                + "  GROUP BY mapKey, xy, borYear "
                + ") "
                + "SELECT "
                + "  mapKey, "
                + "  %s, " // XY coordinates for all zooms
                + "  borYear, "
                + "  occCount "
                + "FROM global "
                + "WHERE xy.x IS NOT NULL AND xy.y IS NOT NULL ",
            epsg, maxZoom, sourceTable, mapKeysSQL, whereSQL, zoomColumns);

    Dataset<Row> input = spark.sql(sql);

    spark.sql("DROP TABLE IF EXISTS " + sourceTable + "_" + suffix + " PURGE");
    input
        .write()
        .mode(SaveMode.Overwrite)
        .format("parquet")
        .saveAsTable(sourceTable + "_" + suffix);
  }

  /** Collects the aggregated data on each pixel for all tiles. */
  private Dataset<Row> prepareToTiles(String epsg, int z, String suffix) {
    String sql =
        String.format(
            "      WITH t1 AS ("
                + "  SELECT "
                + "    mapKey, "
                + "    xy%d AS xy, "
                + "    struct(borYear, sum(occCount) AS occCount) AS feature "
                + "  FROM %s "
                + "  GROUP BY mapKey, xy, borYear"
                + "), "
                + "t2 AS ( "
                + "  SELECT "
                + "    mapKey, "
                + "    xy, "
                + "    collect_list(feature) AS features "
                + "  FROM t1 "
                + "  GROUP BY mapKey, xy "
                + ") "
                + "SELECT "
                + "    hbaseKey(mapKey, %d, tile.tileX, tile.tileY) AS key,"
                + "    collect_list("
                + "      struct(tile.pixelX AS x, tile.pixelY AS y, features)"
                + "    ) AS tile "
                + "  FROM t2 "
                + "    LATERAL VIEW explode("
                + "      collectToTiles('%s', %d, xy.x, xy.y)"
                + "    ) t AS tile "
                + "  GROUP BY key",
            z, sourceTable + "_" + suffix, z, epsg, z);
    return spark.sql(sql);
  }

  /** Run the grouping functions, create the vector tile and write into HFiles. */
  private void generateTiles(String epsg, int z, String sourceTable, String dir) {
    Dataset<Row> tileData = prepareToTiles(epsg, z, sourceTable);
    JavaPairRDD<String, byte[]> mvts = generateMVTs(tileData);
    writeHFiles(mvts, dir, epsg);
    mvts.unpersist(true); // defensive, help spark memory
    tileData.unpersist(true);
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
    JavaPairRDD<ImmutableBytesWritable, KeyValue> data =
        mvts.repartitionAndSortWithinPartitions(new SaltPrefixPartitioner(salter.saltCharCount()))
            .mapToPair(
                (PairFunction<Tuple2<String, byte[]>, ImmutableBytesWritable, KeyValue>)
                    kvp -> {
                      byte[] saltedRowKey = Bytes.toBytes(kvp._1);
                      byte[] mvt = kvp._2;
                      ImmutableBytesWritable key = new ImmutableBytesWritable(saltedRowKey);
                      KeyValue row = new KeyValue(saltedRowKey, colFamily, col, mvt);
                      return new Tuple2<>(key, row);
                    });
    data.saveAsNewAPIHadoopFile(
        dir, ImmutableBytesWritable.class, KeyValue.class, HFileOutputFormat2.class, hadoopConf);
  }

  /** Encodes the source into a dictionary on Integers. */
  static Map<String, Integer> dictionaryEncode(TreeSet<String> source) {
    Map<String, Integer> encoded = new HashMap<>();
    int i = 0;
    for (String s : source) encoded.put(s, i++);
    return encoded;
  }

  /** Inverts the source dictionary to allow for decoding. */
  static Map<Integer, String> invert(Map<String, Integer> source) {
    Map<Integer, String> inverted = new HashMap<>();
    for (Map.Entry<String, Integer> e : source.entrySet()) inverted.put(e.getValue(), e.getKey());
    return inverted;
  }

  /** Set the spark description for the UI. */
  void setDescription(String group, String message) {
    spark.sparkContext().setJobGroup(group, message, true);
  }
}
