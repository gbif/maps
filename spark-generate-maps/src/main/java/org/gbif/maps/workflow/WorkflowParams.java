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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import com.google.common.base.Throwables;

import lombok.Data;
import lombok.NoArgsConstructor;

/** Utility container for passing the WF context params around and through Oozie. */
@Data
@NoArgsConstructor
public class WorkflowParams {
  public static String OOZIE_ZK_QUORUM = "gbif.map.zk.quorum";
  public static String OOZIE_TIMESTAMP = "gbif.map.timestamp";
  public static String OOZIE_SNAPSHOT_DIRECTORY = "gbif.map.snapshotDirectory";
  public static String OOZIE_SOURCE_SUBDIRECTORY = "gbif.map.sourceSubdirectory";
  public static String OOZIE_TARGET_TABLE_PREFIX = "gbif.map.targetTablePrefix";
  public static String OOZIE_TARGET_TABLE = "gbif.map.targetTable";
  public static String OOZIE_TARGET_DIRECTORY = "gbif.map.targetDirectory";
  public static String OOZIE_MODE = "gbif.map.mode";
  public static String OOZIE_KEY_SALT_MODULUS = "gbif.map.keySaltModulus";
  public static String OOZIE_ZK_MAP_METADATA_PATH = "gbif.map.zk.metadataPath";
  public static String OOZIE_ZK_HDFS_LOCK_ZK_PATH = "gbif.map.hdfslock.zkConnectionString";

  private String zkQuorum;
  private String timestamp;
  private String snapshotDirectory;
  private String sourceSubdirectory;
  private String targetTablePrefix;
  private String targetTable;
  private String targetDirectory;
  private String mode;
  private int keySaltModulus;
  private String zkMetaDataPath;
  private String hdfsLockZkConnectionString;

  /** Builder for the main() args. */
  public static WorkflowParams buildForPrepare(String args[]) {
    try {
      WorkflowParams params = new WorkflowParams();
      params.zkQuorum = args[0];
      params.snapshotDirectory = args[1]; // e.g. hdfs://ha-nn/data/hdfsview/
      params.sourceSubdirectory = args[2]; // e.g. occurrence/
      params.targetTablePrefix =
          args[3]; // e.g. prod_a_maps (will receive a suffix such as _tiles_20170101_1343)
      params.mode = args[4]; // tiles | points
      String keySaltModulusAsString = args[5]; // the number of partitions for the HBase table
      params.keySaltModulus = Integer.parseInt(keySaltModulusAsString);
      params.timestamp = new SimpleDateFormat("yyyyMMdd_HHmm").format(new Date());
      params.targetTable = params.targetTablePrefix + "_" + params.mode + "_" + params.timestamp;
      params.targetDirectory =
          args[6] + "_" + params.timestamp; // location where HFiles will be placed
      params.zkMetaDataPath = args[7];
      params.hdfsLockZkConnectionString = args[8];

      return params;

    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /** Builder from the Oozie serialised properties */
  public static WorkflowParams buildFromOozie(String encoded) {
    try (StringReader sr = new StringReader(encoded); ) {
      WorkflowParams params = new WorkflowParams();
      Properties props = new Properties();
      props.load(sr);
      params.zkQuorum = props.getProperty(OOZIE_ZK_QUORUM);
      params.snapshotDirectory = props.getProperty(OOZIE_SNAPSHOT_DIRECTORY);
      params.sourceSubdirectory = props.getProperty(OOZIE_SOURCE_SUBDIRECTORY);
      params.targetTablePrefix = props.getProperty(OOZIE_TARGET_TABLE_PREFIX);
      params.mode = props.getProperty(OOZIE_MODE);
      String keySaltModulusAsString = props.getProperty(OOZIE_KEY_SALT_MODULUS);
      params.keySaltModulus = Integer.parseInt(keySaltModulusAsString);
      params.timestamp = props.getProperty(OOZIE_TIMESTAMP);
      params.targetTable = props.getProperty(OOZIE_TARGET_TABLE);
      params.targetDirectory = props.getProperty(OOZIE_TARGET_DIRECTORY);
      params.zkMetaDataPath = props.getProperty(OOZIE_ZK_MAP_METADATA_PATH);
      params.hdfsLockZkConnectionString = props.getProperty(OOZIE_ZK_HDFS_LOCK_ZK_PATH);

      return params;

    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
  }

  public void saveToOozie() throws IOException {
    String oozieProp = System.getProperty("oozie.action.output.properties");
    if (oozieProp != null) {
      File propFile = new File(oozieProp);
      Properties props = new Properties();
      try (OutputStream os = new FileOutputStream(propFile)) {

        props.setProperty(OOZIE_ZK_QUORUM, getZkQuorum());
        props.setProperty(OOZIE_TIMESTAMP, getTimestamp());
        props.setProperty(OOZIE_SNAPSHOT_DIRECTORY, getSnapshotDirectory());
        props.setProperty(OOZIE_SOURCE_SUBDIRECTORY, getSourceSubdirectory());
        props.setProperty(OOZIE_TARGET_TABLE_PREFIX, getTargetTablePrefix());
        props.setProperty(OOZIE_TARGET_TABLE, getTargetTable());
        props.setProperty(OOZIE_TARGET_DIRECTORY, getTargetDirectory());
        props.setProperty(OOZIE_MODE, getMode());
        props.setProperty(OOZIE_KEY_SALT_MODULUS, String.valueOf(getKeySaltModulus()));
        props.setProperty(OOZIE_ZK_MAP_METADATA_PATH, getZkMetaDataPath());
        props.setProperty(OOZIE_ZK_HDFS_LOCK_ZK_PATH, getHdfsLockZkConnectionString());
        props.store(os, ""); // persist

      } catch (FileNotFoundException e) {
        System.err.println("Unable to save params to Oozie");
        e.printStackTrace();
        throw Throwables.propagate(e); // propagate error and fail the job
      }
    } else {
      throw new RuntimeException("oozie.action.output.properties must be set");
    }
  }
}
