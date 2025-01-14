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
  private final String hivePrefix;
  private final String timestamp;
  private final int tileSize;
  private final int maxZoom;
  private final String clickhouseEndpoint;
  private final String clickhouseDatabase;
  private final String clickhouseUsername; // for replacing tables
  private final String clickhousePassword;
  private final String clickhouseReadOnlyUser; // for granting access for the vector tile server
  private final Boolean clickhouseEnableConnectionPool;
  private final Integer clickhouseSocketTimeout; // (1000 * 60 * 60);
  private final Integer clickhouseConnectTimeout; // (1000 * 60 * 60);

  // dimensions for the map cube
  private static final String DIMENSIONS =
      "datasetKey, publishingOrgKey, publishingCountry, networkKey, countryCode, basisOfRecord, kingdomKey, phylumKey,"
          + " classKey, orderKey, familyKey, genusKey, speciesKey, taxonKey, year";

  public static void main(String[] args) throws Exception {
    // This method intentionally left in for development.
    // Start by creating the HDFS snapshot.
    ClickhouseMapBuilder builder =
        ClickhouseMapBuilder.builder()
            .sourceDir("/data/hdfsview/occurrence/.snapshot/dave-occurrence-map/occurrence/*.avro")
            .hiveDB("dave")
            .hivePrefix("map_clickhouse")
            .tileSize(1024)
            .maxZoom(16)
            .clickhouseEndpoint("http://clickhouse.gbif-dev.org:8123/")
            .clickhouseUsername("default")
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

    LOG.info("Using Hive DB: {}", hiveDB);
    spark.sql("use " + hiveDB);

    spark.sparkContext().conf().set("hive.exec.compress.output", "true");

    // Surface the avro files as a table
    String sourceTable = String.format("%s_avro", hivePrefix);
    LOG.info("Using source table: {}", sourceTable);
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

    LOG.info("Reading avro files from {}", sourceDir);
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

  public void loadClickhouse() throws Exception {
    try (Client client =
            new Client.Builder()
                .addEndpoint(clickhouseEndpoint)
                .setUsername(clickhouseUsername)
                .setPassword(clickhousePassword)
                .enableConnectionPool(clickhouseEnableConnectionPool)
                .setSocketTimeout(clickhouseSocketTimeout)
                .setConnectTimeout(clickhouseConnectTimeout)
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

      final String database = String.format("%s_%s", clickhouseDatabase, timestamp);

      String createDatabase = String.format("CREATE DATABASE %s;", database);
      LOG.info("Clickhouse - executing SQL: {}", createDatabase);
      client.execute(createDatabase).get(1, TimeUnit.HOURS);

      replaceClickhouseTable(client, "mercator", createHive, createLocal, database);
      replaceClickhouseTable(client, "wgs84", createHive, createLocal, database);
      replaceClickhouseTable(client, "arctic", createHive, createLocal, database);
      replaceClickhouseTable(client, "antarctic", createHive, createLocal, database);

      ExecutorService EXEC = Executors.newCachedThreadPool();
      List<Callable<CompletableFuture<CommandResponse>>> tasks = new ArrayList<>();
      tasks.add(() -> client.execute(String.format(loadLocal, database, "mercator")));
      tasks.add(() -> client.execute(String.format(loadLocal, database, "wgs84")));
      tasks.add(() -> client.execute(String.format(loadLocal, database, "arctic")));
      tasks.add(() -> client.execute(String.format(loadLocal, database, "antarctic")));
      LOG.info("Invoking concurrent load");
      List<Future<CompletableFuture<CommandResponse>>> results = EXEC.invokeAll(tasks);
      for (Future<CompletableFuture<CommandResponse>> result : results) {
        result.get(1, TimeUnit.HOURS);
      }
      LOG.info("Finished loading clickhouse!");

      LOG.info("Cleanup HDFS temp tables...");
      List.of("mercator", "wgs84", "arctic", "antarctic")
          .forEach(
              projection -> {
                try {
                  String sql =
                      String.format(
                          "DROP TABLE IF EXISTS %s.hdfs_%s TO %s", database, projection);
                  LOG.info("Clickhouse - executing SQL: {}", sql);
                  client.execute(sql).get(1, TimeUnit.HOURS);
                } catch (Exception e) {
                  LOG.error("Failed to grant access to clickhouse", e);
                }
              });
    }
  }

  /**
   * Recreates the clickhouse table for te provided projection by mounting tables on the hive
   * warehouse and doing a copy.
   */
  private void replaceClickhouseTable(
      Client client, String projection, String createHive, String createLocal, String clickhouseDB)
      throws Exception {

    LOG.info("Starting table preparation for {}", projection);
    // TODO: review how completable futures are used here. Seems odd...

    String dropHive = String.format("DROP TABLE IF EXISTS %s.hdfs_%s;", clickhouseDB, projection);
    LOG.info("Clickhouse - executing SQL: {}", dropHive);
    client.execute(dropHive).get(1, TimeUnit.HOURS);

    String createHiveSQL = String.format(createHive, clickhouseDB, projection);
    LOG.info("Clickhouse - executing SQL: {}", createHiveSQL);
    client.execute(createHiveSQL).get(1, TimeUnit.HOURS);

    String dropCHTable =
        String.format("DROP TABLE IF EXISTS %s.occurrence_%s;", clickhouseDB, projection);
    LOG.info("Clickhouse - executing SQL: {}", dropCHTable);
    client.execute(dropCHTable).get(1, TimeUnit.HOURS);

    String createCHTable = String.format(createLocal, clickhouseDB, projection);
    LOG.info("Clickhouse - executing SQL: {}", createCHTable);
    client.execute(createCHTable).get(1, TimeUnit.HOURS);

    String grant =
        String.format(
            "GRANT SELECT ON %s.occurrence_%s TO %s",
            clickhouseDB, projection, clickhouseReadOnlyUser);

    LOG.info("Clickhouse - executing SQL: {}", grant);
    client.execute(grant).get(1, TimeUnit.HOURS);
  }
}
