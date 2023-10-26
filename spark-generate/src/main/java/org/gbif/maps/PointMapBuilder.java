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
import org.gbif.maps.udf.EncodeBorYearUDF;
import org.gbif.maps.udf.HBaseKeyUDF;
import org.gbif.maps.udf.MapKeysUDF;

import java.io.Serializable;
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
import scala.Tuple2;

/** Builds HFiles for those map views that are small enough to not require a tile pyramid. */
@Builder
class PointMapBuilder implements Serializable {
  private final SparkSession spark;
  private final Set<String> largeMapKeys;
  private final ModulusSalt salter;
  private final String sourceTable;
  private final String targetDir;
  private final Configuration hadoopConf;

  void generate() {
    MapKeysUDF.register(spark, "mapKeys", largeMapKeys, false);
    HBaseKeyUDF.registerPointKey(spark, "hbaseKey", salter);
    EncodeBorYearUDF.register(spark, "encodeBorYear");

    Dataset<Row> countsByLocation =
        spark.sql(
            String.format(
                "        SELECT "
                    + "    hbaseKey(mapKey) AS mapKey, "
                    + "    decimalLatitude AS lat, "
                    + "    decimalLongitude AS lng, "
                    + "    encodeBorYear(basisOfRecord, year) AS borYear, "
                    + "    count(*) AS occCount "
                    + "  FROM "
                    + "    %s m "
                    + "    LATERAL VIEW explode(  "
                    + "      mapKeys("
                    + "        kingdomKey, phylumKey, classKey, orderKey, familyKey, genusKey, speciesKey, taxonKey,"
                    + "        datasetKey, publishingOrgKey, countryCode, publishingCountry, networkKey"
                    + "      ) "
                    + "    ) m AS mapKey "
                    + "  GROUP BY mapKey, lat, lng, borYear",
                sourceTable));
    countsByLocation.createOrReplaceTempView("countsByLocation");

    Dataset<Row> mapData =
        spark.sql(
            "SELECT "
                + "    mapKey, "
                + "    collect_list(struct(lat, lng, borYear, occCount)) AS features"
                + "  FROM countsByLocation"
                + "  GROUP BY mapKey");

    // Create the protobuf tiles.
    // UDF avoided due to the scala performance of wrapping around byte[]
    JavaPairRDD<String, byte[]> encoded =
        mapData
            .javaRDD()
            .mapToPair(
                row -> {
                  String saltedKey = row.getString(0);
                  byte[] pb = ProtobufTiles.generate(row);
                  return new Tuple2<>(saltedKey, pb);
                })
            .repartitionAndSortWithinPartitions(new SaltPrefixPartitioner(salter.saltCharCount()));

    // Convert and write HFiles
    encoded
        .mapToPair(
            (PairFunction<Tuple2<String, byte[]>, ImmutableBytesWritable, KeyValue>)
                kvp -> {
                  byte[] saltedRowKey = Bytes.toBytes(kvp._1);
                  byte[] tile = kvp._2;
                  ImmutableBytesWritable key = new ImmutableBytesWritable(saltedRowKey);
                  KeyValue row =
                      new KeyValue(
                          saltedRowKey,
                          Bytes.toBytes("EPSG_4326"),
                          Bytes.toBytes("features"),
                          tile);
                  return new Tuple2<>(key, row);
                })
        .saveAsNewAPIHadoopFile(
            targetDir + "/points",
            ImmutableBytesWritable.class,
            KeyValue.class,
            HFileOutputFormat2.class,
            hadoopConf);
  }
}
