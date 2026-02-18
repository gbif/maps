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
package org.gbif.maps.workflow;

import org.gbif.maps.common.meta.MapMetastore;
import org.gbif.maps.common.meta.MapTables;
import org.gbif.maps.common.meta.Metastores;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.tool.BulkLoadHFilesTool;
import org.jetbrains.annotations.NotNull;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * Finalising the backfill involves the following:
 *
 * <ol>
 *   <li>Bulk loading the HFiles into the target table
 *   <li>Updating the Maps metastore table (ZK)
 *   <li>Deleting the snapshot table
 *   <li>Deleting the intermediate folders
 * </ol>
 */
@Slf4j
public class FinaliseBackfill {
  private static final String[] PROJECTIONS = {"EPSG_3857", "EPSG_4326", "EPSG_3575", "EPSG_3031"};

  private static final int MAX_ZOOM = 16;

  public static void main(String[] args) throws Exception {
    MapConfiguration config = MapConfiguration.build(args[0]);
    config.setTimestamp(args[1]);
    loadTable(config); // load HBase (throws exception on error)
    updateMeta(config); // update the metastore in ZK
    cleanup(config); // delete old tables, maintaining only previous one disabled
  }

  private static void updateMeta(MapConfiguration config) throws Exception {
    // 1 sec retries
    Configuration hbaseConfig = HBaseConfiguration.create();
    try (MapMetastore metastore =
        Metastores.newZookeeperMapsMeta(
            hbaseConfig.get("hbase.zookeeper.quorum"), 1000, config.getMetadataPath())) {
      MapTables existingMeta = metastore.read(); // we update any existing values
      existingMeta = existingMeta == null ? MapTables.newEmpty() : existingMeta;
      MapTables newMeta = getNewMeta(config, existingMeta);
      log.info("Updating metadata with: " + newMeta);
      metastore.update(newMeta);
    }
  }

  /** Merges the changes we have applied into the existing metadata to write the shared state */
  @NotNull
  private static MapTables getNewMeta(MapConfiguration config, MapTables existingMeta) {
    if ("points".equalsIgnoreCase(config.getMode())) {
      return existingMeta.copyWithNewPoint(config.getFQTableName());

    } else {
      // replace all tables generated in our configuration
      MapTables newMeta = existingMeta.copyWithNewTile(config.getFQTableName());
      for (Map.Entry<String, String> e : config.getChecklistsToProcess().entrySet()) {
        // keyed on UUID to make it easy to integrate with the HTTP param (&checklistKey=<uuid>)
        // alias in the HBase table name to make it easier for HBase admins
        newMeta =
            newMeta.copyWithNewChecklistTable(
                e.getValue(), config.getFQChecklistTableName(e.getKey()));
      }
      return newMeta;
    }
  }

  private static void loadTable(MapConfiguration config) throws Exception {
    Configuration conf = HBaseConfiguration.create();

    BulkLoadHFilesTool loader = new BulkLoadHFilesTool(conf);

    if ("points".equalsIgnoreCase(config.getMode())) {
      TableName table = TableName.valueOf(config.getFQTableName());
      Path hfiles = new Path(config.getFQTargetDirectory(), new Path("points"));
      log.info("Loading HBase table[{}] from [{}]", config.getFQTableName(), hfiles);
      loader.bulkLoad(table, hfiles);

    } else {
      for (String projection : PROJECTIONS) {
        for (int zoom = 0; zoom <= MAX_ZOOM; zoom++) {
          if (config.isProcessNonChecklistTiles()) {
            TableName table = TableName.valueOf(config.getFQTableName());
            bulkLoad(config, table, loader, projection, zoom, "nonTaxon");
          }
          for (String alias : config.getChecklistsToProcess().keySet()) {
            TableName table = TableName.valueOf(config.getFQChecklistTableName(alias));
            bulkLoad(config, table, loader, projection, zoom, "checklist-" + alias);
          }
        }
      }
    }
  }

  /** Bulk loads a single zoom/type combination. */
  private static void bulkLoad(
      MapConfiguration config,
      TableName table,
      BulkLoadHFilesTool loader,
      String projection,
      int zoom,
      String type)
      throws IOException {
    Path hfiles =
        new Path(
            config.getFQTargetDirectory(),
            new Path("tiles", new Path(projection, new Path(type, "z" + zoom))));
    log.info("Zoom[{}] Loading HBase table[{}] from [{}]", zoom, config.getFQTableName(), hfiles);
    loader.bulkLoad(table, hfiles);
  }

  /** Deletes the snapshot and old tables whereby we keep the 2 latest tables only. */
  private static void cleanup(MapConfiguration config) throws Exception {
    // remove all but the last 2 tables, disable all but the last 1 table.
    Configuration hbaseConfig = HBaseConfiguration.create();
    try {
      log.info("Connecting to HBase");
      try (Connection connection = ConnectionFactory.createConnection(hbaseConfig);
          Admin admin = connection.getAdmin();
          // 1 sec retries
          MapMetastore metastore =
              Metastores.newZookeeperMapsMeta(
                  hbaseConfig.get("hbase.zookeeper.quorum"), 1000, config.getMetadataPath())) {

        if (config.getMode().equalsIgnoreCase("points")) {
          String tablesPattern =
            config.getHbase().getTableName() + "_points_\\d{8}_\\d{4}";
            removeOldTables(config, admin, metastore, tablesPattern);
        } else if (config.getMode().equalsIgnoreCase("tiles")) {

          if (config.isProcessNonChecklistTiles()) {
            String tablesPattern =
              config.getHbase().getTableName() + "_tiles_\\d{8}_\\d{4}";
            removeOldTables(config, admin, metastore, tablesPattern);
          }

          for (String alias : config.getChecklistsToProcess().keySet()) {
            String tablesPattern =
              config.getHbase().getTableName() + "_tiles_" + alias + "_\\d{8}_\\d{4}";
            removeOldTables(config, admin, metastore, tablesPattern);
          }
        }
      }
    } catch (IOException e) {
      log.error("Unable to clean HBase tables", e);
      throw e; // deliberate log and throw to keep logs together
    }
  }

  private static void removeOldTables(MapConfiguration config, Admin admin, MapMetastore metastore, String tablesPattern) throws Exception {
    TableName[] tables = admin.listTableNames(tablesPattern);
    // TableName does not order lexigraphically by default
    Arrays.sort(tables, Comparator.comparing(TableName::getNameAsString));

    log.info(
        "Table list: {}",
        Arrays.stream(tables).map(TableName::getNameAsString).collect(Collectors.joining(",")));

    MapTables meta = metastore.read();
    log.info("Current live tables[{}]", meta);

    for (int i = 0; i < tables.length - 1; i++) {
      // Defensive coding: read the metastore each time to minimise possible misuse resulting in
      // race conditions
      meta = metastore.read();

      // Defensive coding: don't delete anything that is the intended target, or currently in
      // use
      // TODO: these can throw NPE
      if (!config.getFQTableName().equalsIgnoreCase(tables[i].getNameAsString())
          && !meta.getPointTable().equalsIgnoreCase(tables[i].getNameAsString())
          && !meta.getTileTable().equalsIgnoreCase(tables[i].getNameAsString())) {

        log.info("Disabling HBase table[{}]", tables[i].getNameAsString());
        if (!admin.isTableDisabled(tables[i])) {
          admin.disableTable(tables[i]);
        }

        // Delete all but the previous table
        if (i < tables.length - 2) {
          log.info("Deleting HBase table[{}]", tables[i].getNameAsString());
          admin.deleteTable(tables[i]);
        }
      }
    }
  }

  @SneakyThrows
  private static FileSystem getHdfsFileSystem() {
    Configuration hdfsConfig = getHdfsConfig();
    return FileSystem.get(hdfsConfig);
  }

  private static Configuration getHdfsConfig() {
    Configuration hdfsConfig = new Configuration();
    hdfsConfig.addResource("core-site.xml");
    hdfsConfig.addResource("hdfs-site.xml");
    return hdfsConfig;
  }
}
