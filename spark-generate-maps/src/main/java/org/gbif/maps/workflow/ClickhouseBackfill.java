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

import org.gbif.maps.ClickhouseMapBuilder;

import java.io.IOException;
import java.util.UUID;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClickhouseBackfill {

  /** Expects [tiles,points] configFile [airflowProperties] */
  public static void main(String[] args) throws Exception {
    if (args.length != 3 && args.length != 4) {
      throw new IllegalArgumentException(
          "Expects [tiles,points] configFile timestamp [airflowProperties]");
    }

    MapConfiguration config = MapConfiguration.build(args[1]);
    config.setTimestamp(args[2]);
    config.setMode(args[0]);
    String snapshotName = UUID.randomUUID().toString();
    log.info("Creating snapshot {} {}", config.getSnapshotDirectory(), snapshotName);

    Configuration hadoopConfiguration = new Configuration();

    Path snapshotPath =
        createHdfsSnapshot(
            hadoopConfiguration,
            config.getSnapshotDirectory(),
            snapshotName,
            config.getHdfsLockConfig());
    String snapshotSource = snapshotPath + "/" + config.getSourceSubdirectory();
    log.info("Created snapshot source, {}", snapshotSource);

    try {
      ClickhouseMapBuilder builder =
          ClickhouseMapBuilder.builder()
              .sourceDir(snapshotSource)
               .timestamp(config.getTimestamp())
              .hiveDB(config.getHiveDB())
              .hivePrefix(config.getClickhouse().getHivePrefix())
              .tileSize(config.getClickhouse().getTileSize())
              .maxZoom(config.getClickhouse().getMaxZoom())
              .clickhouseEndpoint(config.getClickhouse().getEndpoint())
              .clickhouseDatabase(config.getClickhouse().getDatabase())
              .clickhouseUsername(config.getClickhouse().getUsername())
              .clickhousePassword(config.getClickhouse().getPassword())
              .clickhouseReadOnlyUser(config.getClickhouse().getReadOnlyUser())
              .clickhouseEnableConnectionPool(config.getClickhouse().getEnableConnectionPool())
              .clickhouseConnectTimeout(config.getClickhouse().getConnectTimeout())
              .clickhouseSocketTimeout(config.getClickhouse().getSocketTimeout())
              .build();

      log.info("Preparing data in Spark");
      builder.prepareInSpark();
      log.info("Preparing and loading Clickhouse");
      builder.loadClickhouse();

    } finally {
      log.info("Deleting snapshot {} {}", config.getSnapshotDirectory(), snapshotName);
      deleteHdfsSnapshot(hadoopConfiguration, config.getSnapshotDirectory(), snapshotName);
    }
  }

  /** Creates a non-started instance of {@link CuratorFramework}. */
  private static CuratorFramework buildCurator(MapConfiguration.HdfsLockConfig config) {
    Configuration hbaseConf = HBaseConfiguration.create();
    return CuratorFrameworkFactory.builder()
        .namespace(config.getNamespace())
        .retryPolicy(new ExponentialBackoffRetry(config.getSleepTimeMs(), config.getMaxRetries()))
        .connectString(hbaseConf.get("hbase.zookeeper.quorum"))
        .build();
  }

  /** Create a HDFS Snapshot to the input directory. */
  private static Path createHdfsSnapshot(
      Configuration hadoopConfiguration,
      String directory,
      String snapshotName,
      MapConfiguration.HdfsLockConfig hdfsLockConfig)
      throws Exception {

    // barrier since crawling may be running
    try (CuratorFramework curator = buildCurator(hdfsLockConfig)) {
      curator.start();
      String lockPath = hdfsLockConfig.getLockingPath() + hdfsLockConfig.getLockName();
      DistributedBarrier barrier = new DistributedBarrier(curator, lockPath);
      log.info("Acquiring barrier {}", lockPath);
      barrier.waitOnBarrier();
      log.info("Setting barrier {}", lockPath);
      barrier.setBarrier();

      // the actual task
      FileSystem fs = FileSystem.get(hadoopConfiguration);
      Path result = fs.createSnapshot(new Path(directory), snapshotName);

      log.info("Removing barrier {}", lockPath);
      barrier.removeBarrier();

      return result;
    }
  }

  /** Deletes a HDFS Snapshot to the input directory. */
  private static void deleteHdfsSnapshot(
      Configuration hadoopConfiguration, String directory, String snapshotName) throws IOException {
    FileSystem fs = FileSystem.get(hadoopConfiguration);
    fs.deleteSnapshot(new Path(directory), snapshotName);
  }
}
