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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.tool.BulkLoadHFilesTool;

import lombok.extern.slf4j.Slf4j;

/**
 * Finalising the backfill involves the following:
 *
 * <ol>
 *   <li>Bulkloading the HFiles into the target table
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
    MapConfiguration config = MapConfiguration.build(args[1]);
    config.setTimestamp(args[2]);
    config.setMode(args[0]);
    loadTable(config); // load HBase (throws exception on error)
    updateMeta(config); // update the metastore in ZK
    cleanup(config);
  }

  private static void updateMeta(MapConfiguration config) throws Exception {
    // 1 sec retries
    Configuration hbaseConfig = HBaseConfiguration.create();
    try (MapMetastore metastore =
        Metastores.newZookeeperMapsMeta(
            hbaseConfig.get("hbase.zookeeper.quorum"), 1000, config.getMetadataPath())) {

      MapTables meta = metastore.read(); // we update any existing values

      // NOTE: there is the possibility of a race condition here if 2 instances are updating
      // different
      // modes simultaneously
      if ("points".equalsIgnoreCase(config.getMode())) {
        MapTables newMeta =
            new MapTables(
                (meta == null) ? null : meta.getTileTable(), config.getFQTableName());
        log.info("Updating metadata with: " + newMeta);
        metastore.update(newMeta);

      } else {
        MapTables newMeta =
            new MapTables(
                config.getFQTableName(), (meta == null) ? null : meta.getPointTable());
        log.info("Updating metadata with: " + newMeta);
        metastore.update(newMeta);
      }
    }
  }

  private static void loadTable(MapConfiguration config) throws Exception {
    Configuration conf = HBaseConfiguration.create();

    TableName table = TableName.valueOf(config.getFQTableName());
    BulkLoadHFilesTool loader = new BulkLoadHFilesTool(conf);

    if ("points".equalsIgnoreCase(config.getMode())) {
      Path hfiles = new Path(config.getFQTargetDirectory(), new Path("points"));
      log.info("Loading HBase table[" + config.getFQTableName() + "] from [" + hfiles + "]");
      loader.bulkLoad(table, hfiles);

    } else {
      for (String projection : PROJECTIONS) {
        for (int zoom = 0; zoom <= MAX_ZOOM; zoom++) {
          Path hfiles =
              new Path(
                  config.getFQTargetDirectory(),
                  new Path("tiles", new Path(projection, "z" + zoom)));
          log.info(
              "Zoom["
                  + zoom
                  + "] Loading HBase table["
                  + config.getFQTableName()
                  + "] from ["
                  + hfiles
                  + "]");
          loader.bulkLoad(table, hfiles);
        }
      }
    }
  }

  /** Deletes the snapshot and old tables whereby we keep the 2 latest tables only. */
  private static void cleanup(MapConfiguration config) throws Exception {
    Configuration hbaseConfig = HBaseConfiguration.create();
    try {
      log.info("Connecting to HBase");
      try (Connection connection = ConnectionFactory.createConnection(hbaseConfig);
          Admin admin = connection.getAdmin();
          // 1 sec retries
          MapMetastore metastore =
              Metastores.newZookeeperMapsMeta(
                  hbaseConfig.get("hbase.zookeeper.quorum"),
                  1000,
                 config.getMetadataPath())) {

        // remove all but the last 2 tables
        // table names are suffixed with a timestamp e.g. prod_d_maps_points_20180616_1320
        String tablesPattern =
            config.getHbase().getTableName() + "_" + config.getMode() + "_\\d{8}T\\d{6}";
        TableName[] tables = admin.listTableNames(tablesPattern);
        // TableName does not order lexigraphically by default
        Arrays.sort(tables, Comparator.comparing(TableName::getNameAsString));

        MapTables meta = metastore.read();
        log.info("Current live tables[" + meta + "]");

        for (int i = 0; i < tables.length - 2; i++) {
          // Defensive coding: read the metastore each time to minimise possible misue resulting in
          // race conditions
          meta = metastore.read();

          // Defensive coding: don't delete anything that is the intended target, or currently in
          // use
          if (!config.getFQTableName().equalsIgnoreCase(tables[i].getNameAsString())
              && !meta.getPointTable().equalsIgnoreCase(tables[i].getNameAsString())
              && !meta.getTileTable().equalsIgnoreCase(tables[i].getNameAsString())) {

            log.info("Disabling HBase table[" + tables[i].getNameAsString() + "]");
            admin.disableTable(tables[i]);
            log.info("Deleting HBase table[" + tables[i].getNameAsString() + "]");
            admin.deleteTable(tables[i]);
          }
        }
      }
    } catch (IOException e) {
      log.error("Unable to clean HBase tables");
      e.printStackTrace();
      throw e; // deliberate log and throw to keep logs together
    }

    // Cleanup the working directory if in hdfs://nameserver/tmp/* to avoid many small files
    String regex = "hdfs://[-_a-zA-Z0-9]+/tmp/.+"; // defensive, cleaning only /tmp in hdfs
    if (config.getFQTargetDirectory().matches(regex)) {
      Configuration hdfsConfig = new Configuration();
      hdfsConfig.addResource("core-site.xml");
      hdfsConfig.addResource("hdfs-site.xml");
      FileSystem hdfs = FileSystem.get(hdfsConfig);
      try {
        String dir =
            config.getTargetDirectory().substring(config.getFQTargetDirectory().indexOf("/tmp"));
        log.info(
            "Deleting working directory ["
                + config.getFQTargetDirectory()
                + "] which translates to ["
                + "-rm -r -skipTrash "
                + dir
                + "]");
        hdfs.deleteOnExit(new Path(dir));
      } catch (Exception e) {
        throw new IOException("Unable to delete the working directory", e);
      }
    } else {
      log.info(
          "Working directory ["
              + config.getFQTargetDirectory()
              + "] will not be removed automatically "
              + "- only /tmp/* working directories will be cleaned");
    }
  }
}
