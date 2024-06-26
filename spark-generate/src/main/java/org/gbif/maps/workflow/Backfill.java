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

import org.gbif.maps.MapBuilder;

import java.io.IOException;
import java.net.URL;
import java.util.UUID;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.io.Resources;

import lombok.extern.slf4j.Slf4j;

/** The driver for back-filling the map tables from Oozie. */
@Slf4j
public class Backfill {

  /** Expects [tiles,points] configFile [oozieProperties] */
  public static void main(String[] args) throws Exception {
    if (!(args.length == 2 || args.length == 3))
      throw new IllegalArgumentException("Expects [tiles,points] configFile [oozieProperties]");

    log.info("Reading from {}", args[1]);
    URL conf = Resources.getResource(args[1]);
    log.info("Reading from {}", conf);
    MapConfiguration config =
        new ObjectMapper(new YAMLFactory()).readValue(conf, MapConfiguration.class);

    // MapConfiguration config = MapConfiguration.build(args[1]);

    // Oozie does not support templated files, and therefore we opt to override parameters that are
    // calculated at runtime in the Oozie workflow.
    if (args.length == 3) {
      WorkflowParams o = WorkflowParams.buildFromOozie(args[2]);
      config.getHbase().setZkQuorum(o.getZkQuorum());
      config.setSnapshotDirectory(o.getSnapshotDirectory());
      config.setSourceSubdirectory(o.getSourceSubdirectory());
      config.getHbase().setTableName(o.getTargetTable());
      config.setTargetDirectory(o.getTargetDirectory());
      config.getHdfsLockConfig().setZkConnectionString(o.getHdfsLockZkConnectionString());
    }

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
    log.info("Created snapshot, {}", snapshotPath);

    try {
      String mode = args[0]; // tiles or points
      MapBuilder mapBuilder =
          MapBuilder.builder()
              .sourceDir(snapshotSource)
              .hiveDB(config.getHiveDB())
              .hiveInputSuffix(mode.toLowerCase())
              .zkQuorum(config.getHbase().getZkQuorum())
              .hbaseTable(config.getHbase().getTableName())
              .targetDir(config.getTargetDirectory())
              .modulo(config.getHbase().getKeySaltModulus())
              .threshold(config.getTilesThreshold())
              .tileSize(config.getTileSize())
              .bufferSize(config.getTileBufferSize())
              .maxZoom(config.getMaxZoom())
              .buildPoints(mode.equalsIgnoreCase("points") ? true : false)
              .buildTiles(mode.equalsIgnoreCase("tiles") ? true : false)
              .build();
      log.info("Launching map build with config: {}", mapBuilder);
      mapBuilder.run();

    } finally {
      log.info("Deleting snapshot {} {}", config.getSnapshotDirectory(), snapshotName);
      deleteHdfsSnapshot(hadoopConfiguration, config.getSnapshotDirectory(), snapshotName);
    }
  }

  /** Creates a non-started instance of {@link CuratorFramework}. */
  private static CuratorFramework buildCurator(MapConfiguration.HdfsLockConfig config) {
    return CuratorFrameworkFactory.builder()
        .namespace(config.getNamespace())
        .retryPolicy(new ExponentialBackoffRetry(config.getSleepTimeMs(), config.getMaxRetries()))
        .connectString(config.getZkConnectionString())
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
