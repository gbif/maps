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

import org.gbif.maps.common.hbase.ModulusSalt;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;

import lombok.extern.slf4j.Slf4j;

/** An Oozie step to prepare for a backfill, which creates the target table in HBase. */
@Slf4j
public class PrepareBackfill {

  public static void main(String[] args) throws IOException {
    if (args.length > 3) {
      log.info("Starting in Oozie mode");
      WorkflowParams params = WorkflowParams.buildForPrepare(args);
      log.info(params.toString());
      startOozieWorkflow(params);
    } else {
      log.info("Starting in Spark mode");
      String mode = args[0];
      MapConfiguration config = MapConfiguration.build(args[1]);
      startSparkWorkflow(mode, config);
    }
  }

  private static void startOozieWorkflow(WorkflowParams params) throws IOException {
    log.info(params.toString());
    try {
      log.info("Connecting to HBase");
      Configuration conf = HBaseConfiguration.create();
      conf.set(HConstants.ZOOKEEPER_QUORUM, params.getZkQuorum());
      try (Connection connection = ConnectionFactory.createConnection(conf);
          Admin admin = connection.getAdmin()) {

        HTableDescriptor target = new HTableDescriptor(TableName.valueOf(params.getTargetTable()));
        appendColumnFamily(target, "EPSG_4326"); // points and tiles both have this CF
        if ("tiles".equalsIgnoreCase(params.getMode())) {
          appendColumnFamily(target, "EPSG_3857");
          appendColumnFamily(target, "EPSG_3575");
          appendColumnFamily(target, "EPSG_3031");
        }
        ModulusSalt salt = new ModulusSalt(params.getKeySaltModulus());
        System.out.format("Creating %s", params.getTargetTable());
        admin.createTable(target, salt.getTableRegions());
      }

      // update the Oozie WF parameters for future jobs
      params.saveToOozie();

    } catch (IOException e) {
      log.error("Unable to prepare the tables for backfilling");
      e.printStackTrace();
      throw e; // deliberate log and throw to keep logs together
    }
  }

  private static void startSparkWorkflow(String mode, MapConfiguration config) throws IOException {
    try {
      log.info("Connecting to HBase");
      Configuration conf = HBaseConfiguration.create();
      try (Connection connection = ConnectionFactory.createConnection(conf);
          Admin admin = connection.getAdmin()) {

        HTableDescriptor target =
            new HTableDescriptor(TableName.valueOf(config.getHbase().getTableName()));
        appendColumnFamily(target, "EPSG_4326"); // points and tiles both have this CF
        if ("tiles".equalsIgnoreCase(mode)) {
          appendColumnFamily(target, "EPSG_3857");
          appendColumnFamily(target, "EPSG_3575");
          appendColumnFamily(target, "EPSG_3031");
        }
        ModulusSalt salt = new ModulusSalt(config.getHbase().getKeySaltModulus());
        System.out.format("Creating %s", config.getHbase().getTableName());
        admin.createTable(target, salt.getTableRegions());
      }

    } catch (TableExistsException e) {

    } catch (IOException e) {
      log.error("Unable to prepare the tables for backfilling");
      e.printStackTrace();
      throw e; // deliberate log and throw to keep logs together
    }
  }
  /**
   * Sets the column family for the table as per
   * https://github.com/gbif/maps/blob/master/spark-process/README.md
   *
   * @param target The target table
   * @param name The CF name
   */
  private static void appendColumnFamily(HTableDescriptor target, String name) {
    HColumnDescriptor cf = new HColumnDescriptor(name);
    cf.setMaxVersions(1);
    cf.setCompressionType(Compression.Algorithm.GZ);
    cf.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
    cf.setBloomFilterType(BloomType.NONE);
    target.addFamily(cf);
  }
}
