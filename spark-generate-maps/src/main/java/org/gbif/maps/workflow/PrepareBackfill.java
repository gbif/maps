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

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

import lombok.extern.slf4j.Slf4j;

/** An Airflow step to prepare for a backfill, which creates the target table in HBase. */
@Slf4j
public class PrepareBackfill {

  public static void main(String[] args) throws IOException {
    MapConfiguration config = MapConfiguration.build(args[1]);
    config.setTimestamp(args[2]);
    config.setMode(args[0]);
    startSparkWorkflow(config);
  }

  private static void startSparkWorkflow(MapConfiguration config) throws IOException {
    try {
      log.info("Connecting to HBase");
      try (Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
          Admin admin = connection.getAdmin()) {
        TableDescriptorBuilder target =
            TableDescriptorBuilder.newBuilder(TableName.valueOf(config.getFQTableName()));
        target.setMaxFileSize(config.getHbase().getMaxFileSize());
        appendColumnFamily(target, "EPSG_4326"); // points and tiles both have this CF
        if ("tiles".equalsIgnoreCase(config.getMode())) {
          appendColumnFamily(target, "EPSG_3857");
          appendColumnFamily(target, "EPSG_3575");
          appendColumnFamily(target, "EPSG_3031");
        }
        ModulusSalt salt = new ModulusSalt(config.getHbase().getKeySaltModulus());
        log.info("Creating {}", config.getFQTableName());
        admin.createTable(target.build(), salt.getTableRegions());
      }

    } catch (TableExistsException e) {
      log.info("Ignoring non-existing table");
    } catch (IOException e) {
      log.error("Unable to prepare the tables for backfilling");
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
  private static void appendColumnFamily(TableDescriptorBuilder target, String name) {
    ColumnFamilyDescriptorBuilder cf =
        ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(name));
    cf.setMaxVersions(1);
    cf.setCompressionType(Algorithm.SNAPPY);
    cf.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
    cf.setBloomFilterType(BloomType.NONE);
    target.setColumnFamily(cf.build());
  }
}
