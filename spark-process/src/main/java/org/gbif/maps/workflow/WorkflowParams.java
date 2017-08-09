package org.gbif.maps.workflow;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import com.google.common.base.Throwables;

/**
 * Utility container for passing the WF context params around and through Oozie.
 */
public class WorkflowParams {
  public static String OOZIE_ZK_QUORUM = "gbif.map.zk.quorum";
  public static String OOZIE_TIMESTAMP = "gbif.map.timestamp";
  public static String OOZIE_SOURCE_TABLE = "gbif.map.sourceTable";
  public static String OOZIE_SNAPSHOT_TABLE = "gbif.map.snapshotTable";
  public static String OOZIE_TARGET_TABLE_PREFIX = "gbif.map.targetTablePrefix";
  public static String OOZIE_TARGET_TABLE = "gbif.map.targetTable";
  public static String OOZIE_TARGET_DIRECTORY = "gbif.map.targetDirectory";
  public static String OOZIE_MODE = "gbif.map.mode";
  public static String OOZIE_KEY_SALT_MODULUS = "gbif.map.keySaltModulus";

  private String zkQuorum;
  private String timestamp;
  private String sourceTable;
  private String snapshotTable;
  private String targetTablePrefix;
  private String targetTable;
  private String targetDirectory;
  private String mode;
  private int keySaltModulus;

  private WorkflowParams() {}

  /**
   * Builder for the main() args.
   */
  public static WorkflowParams buildForPrepare(String args[]) {
    try {
      WorkflowParams params = new WorkflowParams();
      params.zkQuorum = args[0];
      params.sourceTable = args[1];  // e.g. prod_a_occurrence
      params.targetTablePrefix = args[2];  // e.g. prod_a_maps (will receive a suffix such as _tiles_20170101_1343)
      params.mode = args[3];  // tiles | points
      String keySaltModulusAsString = args[4];  // the number of partitions for the HBase table
      params.keySaltModulus = Integer.parseInt(keySaltModulusAsString);
      params.timestamp = new SimpleDateFormat("yyyyMMdd_HHmm").format(new Date());
      params.snapshotTable = params.sourceTable + "_" + params.timestamp;
      params.targetTable = params.targetTablePrefix + "_" + params.mode + "_" + params.timestamp;
      params.targetDirectory = args[5] + "_" + params.timestamp; // location where HFiles will be placed

      return params;

    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Builder from the Oozie serialised properties
   */
  public static WorkflowParams buildFromOozie(String encoded) {
    try (
      StringReader sr = new StringReader(encoded);
    ) {
      WorkflowParams params = new WorkflowParams();
      Properties props = new Properties();
      props.load(sr);
      params.zkQuorum = props.getProperty(OOZIE_ZK_QUORUM);
      params.sourceTable = props.getProperty(OOZIE_SOURCE_TABLE);
      params.targetTablePrefix = props.getProperty(OOZIE_TARGET_TABLE_PREFIX);
      params.mode = props.getProperty(OOZIE_MODE);
      String keySaltModulusAsString = props.getProperty(OOZIE_KEY_SALT_MODULUS);
      params.keySaltModulus = Integer.parseInt(keySaltModulusAsString);
      params.timestamp = props.getProperty(OOZIE_TIMESTAMP);
      params.snapshotTable = props.getProperty(OOZIE_SNAPSHOT_TABLE);
      params.targetTable = props.getProperty(OOZIE_TARGET_TABLE);
      params.targetDirectory = props.getProperty(OOZIE_TARGET_DIRECTORY);

      return params;

    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
  }


  public String getZkQuorum() {
    return zkQuorum;
  }

  public String getTimestamp() {
    return timestamp;
  }

  public String getSourceTable() {
    return sourceTable;
  }

  public String getSnapshotTable() {
    return snapshotTable;
  }

  public String getTargetTablePrefix() {
    return targetTablePrefix;
  }

  public String getTargetTable() {
    return targetTable;
  }

  public String getMode() {
    return mode;
  }

  public String getTargetDirectory() {
    return targetDirectory;
  }

  public int getKeySaltModulus() {
    return keySaltModulus;
  }

  @Override
  public String toString() {
    return "WorkflowParams{" +
           "zkQuorum='" + zkQuorum + '\'' +
           ", timestamp='" + timestamp + '\'' +
           ", sourceTable='" + sourceTable + '\'' +
           ", snapshotTable='" + snapshotTable + '\'' +
           ", targetTablePrefix='" + targetTablePrefix + '\'' +
           ", targetTable='" + targetTable + '\'' +
           ", targetDirectory='" + targetDirectory + '\'' +
           ", mode='" + mode + '\'' +
           ", keySaltModulus=" + keySaltModulus +
           '}';
  }

  public void saveToOozie() throws IOException {
    String oozieProp = System.getProperty("oozie.action.output.properties");
    if (oozieProp != null) {
      File propFile = new File(oozieProp);
      Properties props = new Properties();
      try (OutputStream os = new FileOutputStream(propFile)) {

        props.setProperty(OOZIE_ZK_QUORUM, getZkQuorum());
        props.setProperty(OOZIE_TIMESTAMP, getTimestamp());
        props.setProperty(OOZIE_SOURCE_TABLE, getSourceTable());
        props.setProperty(OOZIE_SNAPSHOT_TABLE, getSnapshotTable());
        props.setProperty(OOZIE_TARGET_TABLE_PREFIX, getTargetTablePrefix());
        props.setProperty(OOZIE_TARGET_TABLE, getTargetTable());
        props.setProperty(OOZIE_TARGET_DIRECTORY, getTargetDirectory());
        props.setProperty(OOZIE_MODE, getMode());
        props.setProperty(OOZIE_KEY_SALT_MODULUS, String.valueOf(getKeySaltModulus()));
        props.store(os, ""); // persist

      } catch (FileNotFoundException e) {
        System.err.println("Unable to save params to Oozie");
        e.printStackTrace();
        throw Throwables.propagate(e); // deliberate log and throw to propogate error and fail the job
      }
    } else {
      throw new RuntimeException("oozie.action.output.properties is not set - unable to persist action state");
    }
  }

}
