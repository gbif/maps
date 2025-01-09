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
package org.gbif.maps.clickhouse;

import org.gbif.maps.udf.ProjectUDF;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.command.CommandResponse;

import lombok.Builder;

@Builder(toBuilder = true)
public class ClickhouseMapBuilder implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(ClickhouseMapBuilder.class);

  private final String sourceDir;
  private final String hiveDB;
  private final String zkQuorum;
  private final String hivePrefix;
  private final int tileSize;
  private final int maxZoom;
  private final String clickhouseEndpoint;
  private final String clickhouseUser; // for replacing tables
  private final String clickhousePassword;
  private final String clickhouseReadOnlyUser; // for granting access for the vector tile server

  // dimensions for the map cube
  private static final String DIMENSIONS =
      "datasetKey, publishingOrgKey, publishingCountry, networkKey, countryCode, basisOfRecord, kingdomKey, phylumKey,"
          + " classKey, orderKey, familyKey, genusKey, speciesKey, taxonKey, year";

  public static void main(String[] args) throws Exception {
    // This method intentionally left in for development.
    // Start by creating the HDFS snapshot.
    ClickhouseMapBuilder builder =
        ClickhouseMapBuilder.builder()
            .sourceDir("/data/hdfsview/occurrence/.snapshot/tim-occurrence-map/occurrence/*.avro")
            .hiveDB("tim")
            .zkQuorum("c5zk1.gbif.org:2181,c5zk2.gbif.org:2181,c5zk3.gbif.org:2181")
            .hivePrefix("map_clickhouse")
            .tileSize(1024)
            .maxZoom(16)
            .clickhouseEndpoint("http://clickhouse.gbif-dev.org:8123/")
            .clickhouseUser("default")
            .clickhousePassword("clickhouse")
            .clickhouseReadOnlyUser("tim")
            .build();

    LOG.info("Preparing data in Spark");
    builder.prepareInSpark();
    LOG.info("Preparing and loading Clickhouse");
    builder.loadClickhouse();
  }

  /** Establishes a Spark cluster, prepares data, and then releases resources. */
  public void prepareInSpark() {
    SparkSession spark =
        SparkSession.builder().appName("Clickhouse Map Builder").enableHiveSupport().getOrCreate();
    spark.sql("use " + hiveDB);
    spark.sparkContext().conf().set("hive.exec.compress.output", "true");

    // Surface the avro files as a table
    String sourceTable = String.format("%s_avro", hivePrefix);
    readAvroSource(spark, sourceTable);

    // Project the coordinates
    ProjectUDF.register(spark, "project", tileSize);
    String projectedTable = String.format("%s_projected", hivePrefix);
    spark.sql(
        String.format(
            "        CACHE TABLE %1$s AS "
                + "  SELECT "
                + "    project(decimalLatitude, decimalLongitude, 'EPSG:3857', %2$d) AS mercator_xy, "
                + "    project(decimalLatitude, decimalLongitude, 'EPSG:4326', %2$d) AS wgs84_xy, "
                + "    project(decimalLatitude, decimalLongitude, 'EPSG:3575', %2$d) AS arctic_xy, "
                + "    project(decimalLatitude, decimalLongitude, 'EPSG:3031', %2$d) AS antarctic_xy, "
                + "    decimalLatitude,"
                + "    occCount, "
                + "    %3$s "
                + "  FROM %4$s",
            projectedTable, maxZoom, DIMENSIONS, sourceTable));

    // mercator
    replaceTable(
        spark,
        String.format("%s_mercator", hivePrefix),
        String.format(
            "        SELECT mercator_xy.x AS x, mercator_xy.y AS y, %1$s, sum(occCount) AS occCount "
                + "  FROM %2$s "
                + "  WHERE decimalLatitude BETWEEN -85 AND 85"
                + "  GROUP BY x, y, %1$s ",
            DIMENSIONS, projectedTable));

    // wgs84
    replaceTable(
        spark,
        String.format("%s_wgs84", hivePrefix),
        String.format(
            "        SELECT wgs84_xy.x AS x, wgs84_xy.y AS y, %1$s, sum(occCount) AS occCount "
                + "  FROM %2$s "
                + "  GROUP BY x, y, %1$s ",
            DIMENSIONS, projectedTable));

    // arctic
    replaceTable(
        spark,
        String.format("%s_arctic", hivePrefix),
        String.format(
            "        SELECT arctic_xy.x AS x, arctic_xy.y AS y, %1$s, sum(occCount) AS occCount "
                + "  FROM %2$s "
                + "  WHERE decimalLatitude >= 0"
                + "  GROUP BY x, y, %1$s ",
            DIMENSIONS, projectedTable));

    // antarctic
    replaceTable(
        spark,
        String.format("%s_antarctic", hivePrefix),
        String.format(
            "        SELECT antarctic_xy.x AS x, antarctic_xy.y AS y, %1$s, sum(occCount) AS occCount "
                + "  FROM %2$s "
                + "  WHERE decimalLatitude <= 0"
                + "  GROUP BY x, y, %1$s ",
            DIMENSIONS, projectedTable));

    spark.close();
  }

  /**
   * Reads the input avro files, filtering for records of interest, aggregates to the dimensions and
   * coordinates with a count and registers a temp view.
   */
  private void readAvroSource(SparkSession spark, String targetView) {
    spark
        .read()
        .format("com.databricks.spark.avro")
        .load(sourceDir)
        .createOrReplaceTempView("avro_files");
    spark
        .sql(
            String.format(
                "        SELECT decimalLatitude, decimalLongitude, %1$s, count(*) AS occCount"
                    + "  FROM avro_files "
                    + "  WHERE"
                    + "    decimalLatitude IS NOT NULL AND "
                    + "    decimalLongitude IS NOT NULL AND "
                    + "    hasGeospatialIssues = false AND "
                    + "    occurrenceStatus='PRESENT' "
                    + "  GROUP BY decimalLatitude, decimalLongitude, %1$s", // reduces to 1/3
                DIMENSIONS))
        .repartition(spark.sparkContext().conf().getInt("spark.sql.shuffle.partitions", 1200))
        .createOrReplaceTempView(targetView);
  }

  /** Drop the table, execute the query and create the table stored as parquet */
  private static void replaceTable(SparkSession spark, String table, String sql) {
    spark.sql(String.format("DROP TABLE IF EXISTS %s", table));
    spark.sql(sql).write().format("parquet").saveAsTable(table);
  }

  private void loadClickhouse() throws Exception {
    try (Client client =
            new Client.Builder()
                .addEndpoint(clickhouseEndpoint)
                .setUsername(clickhouseUser)
                .setPassword(clickhousePassword)
                .enableConnectionPool(true)
                .setSocketTimeout(1000 * 60 * 60) // TODO: see comments below
                .setConnectTimeout(1000 * 60 * 60) // TODO: see comments below
                .build();
        // clickhouse-java does not support multiline statements
        InputStream hiveSQL =
            this.getClass().getResourceAsStream("/clickhouse/create-hive-table.sql");
        InputStream localSQL =
            this.getClass().getResourceAsStream("/clickhouse/create-local-table.sql");
        InputStream loadSQL =
            this.getClass().getResourceAsStream("/clickhouse/load-local-table.sql")) {

      String createHive =
          new BufferedReader(new InputStreamReader(hiveSQL))
              .lines()
              .collect(Collectors.joining("\n"));
      String createLocal =
          new BufferedReader(new InputStreamReader(localSQL))
              .lines()
              .collect(Collectors.joining("\n"));
      String loadLocal =
          new BufferedReader(new InputStreamReader(loadSQL))
              .lines()
              .collect(Collectors.joining("\n"));

      replaceClickhouseTable(client, "mercator", createHive, createLocal, loadLocal);
      replaceClickhouseTable(client, "wgs84", createHive, createLocal, loadLocal);
      replaceClickhouseTable(client, "arctic", createHive, createLocal, loadLocal);
      replaceClickhouseTable(client, "antarctic", createHive, createLocal, loadLocal);

      LOG.info("Starting data load for mercator");

      /*
      CompletableFuture<QueryResponse> q1 = client.query(String.format(loadLocal, "mercator"));
      LOG.info("Finished data load for mercator");
      LOG.info("Starting data load for wgs84");
      CompletableFuture<CommandResponse> q2 = client.execute(String.format(loadLocal, "wgs84"));
      LOG.info("Finished data load for wgs84");
      LOG.info("Starting data load for arctic");
      CompletableFuture<CommandResponse> q3 = client.execute(String.format(loadLocal, "arctic"));
      LOG.info("Finished data load for arctic");
      LOG.info("Starting data load for antarctic");
      CompletableFuture<CommandResponse> q4 = client.execute(String.format(loadLocal, "antarctic"));
      LOG.info("Finished data load for antarctic");
      */

      ExecutorService EXEC = Executors.newCachedThreadPool();
      List<Callable<CompletableFuture<CommandResponse>>> tasks = new ArrayList<>();
      tasks.add(() -> client.execute(String.format(loadLocal, "mercator")));
      tasks.add(() -> client.execute(String.format(loadLocal, "wgs84")));
      tasks.add(() -> client.execute(String.format(loadLocal, "arctic")));
      tasks.add(() -> client.execute(String.format(loadLocal, "antarctic")));

      LOG.info("Invoking concurrent load");
      List<Future<CompletableFuture<CommandResponse>>> results = EXEC.invokeAll(tasks);
      for (Future<CompletableFuture<CommandResponse>> result : results) {
        result.get(1, TimeUnit.HOURS);
      }
      LOG.info("Finished loading clickhouse!");

      client
          .execute(String.format("DROP TABLE IF EXISTS hdfs_%s", "mercator"))
          .get(1, TimeUnit.HOURS);
      client.execute(String.format("DROP TABLE IF EXISTS hdfs_%s", "wgs84")).get(1, TimeUnit.HOURS);
      client
          .execute(String.format("DROP TABLE IF EXISTS hdfs_%s", "arctic"))
          .get(1, TimeUnit.HOURS);
      client
          .execute(String.format("DROP TABLE IF EXISTS hdfs_%s", "antarctic"))
          .get(1, TimeUnit.HOURS);
    }
  }

  /**
   * Recreates the clickhouse table for te provided projection by mounting tables on the hive
   * warehouse and doing a copy.
   */
  private void replaceClickhouseTable(
      Client client, String projection, String createHive, String createLocal, String loadLocal)
      throws Exception {

    LOG.info("Starting table preparation for {}", projection);
    // TODO: review how completable futures are used here. Seems odd...

    client
        .execute(String.format("DROP TABLE IF EXISTS hdfs_%s;", projection))
        .get(1, TimeUnit.HOURS);
    client.execute(String.format(createHive, projection, hiveDB)).get(1, TimeUnit.HOURS);
    client
        .execute(String.format("DROP TABLE IF EXISTS occurrence_%s;", projection))
        .get(1, TimeUnit.HOURS);
    client.execute(String.format(createLocal, projection)).get(1, TimeUnit.HOURS);
    client
        .execute(
            String.format(
                "GRANT SELECT ON default.occurrence_%s TO %s", projection, clickhouseReadOnlyUser))
        .get(1, TimeUnit.HOURS);
    // LOG.info("Starting data load for {}", projection);
    // client.execute(String.format(loadLocal, projection)).get(1, TimeUnit.HOURS);
    // LOG.info("Finished data load for {}", projection);
    // client.execute(String.format("DROP TABLE IF EXISTS hdfs_%s", projection)).get(1,
    // TimeUnit.HOURS);
  }
}
