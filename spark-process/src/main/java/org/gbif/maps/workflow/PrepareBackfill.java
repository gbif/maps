package org.gbif.maps.workflow;

import org.gbif.maps.common.hbase.ModulusSalt;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.ipc.RpcClientImpl;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.v2.app.MRAppMaster;

/**
 * An Oozie step to prepare for a backfill, which involves the following:
 * <ol>
 *   <li>Snapshot the source table</li>
 *   <li>Create the target table in HBase</li>
 * </ol>
 */
public class PrepareBackfill {

  public static void main(String[] args) throws IOException {
    WorkflowParams params = WorkflowParams.buildForPrepare(args);
    System.out.println(params.toString());
    try {
      System.out.println("Connecting to HBase");
      Configuration conf = HBaseConfiguration.create();
      conf.set(HConstants.ZOOKEEPER_QUORUM, params.getZkQuorum());
      try (
        Connection connection = ConnectionFactory.createConnection(conf);
        MRAppMaster m = null;
        Admin admin = connection.getAdmin();
      ) {
        System.out.format("Snapshotting %s to %s", params.getSourceTable(), params.getSnapshotTable());
        admin.snapshot(params.getSnapshotTable(), TableName.valueOf(params.getSourceTable()));

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
      System.err.println("Unable to prepare the tables for backfilling");
      e.printStackTrace();
      throw e; // deliberate log and throw to keep logs together
    }
  }

  /**
   * Sets the column family for the table as per https://github.com/gbif/maps/blob/master/spark-process/README.md
   * @param target The target table
   * @param name The CF name
   */
  private static void appendColumnFamily(HTableDescriptor target, String name) {
    HColumnDescriptor cf = new HColumnDescriptor(name);
    cf.setMaxVersions(1);
    cf.setCompressionType(Compression.Algorithm.SNAPPY);
    cf.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
    cf.setBloomFilterType(BloomType.NONE);
    target.addFamily(cf);
  }
}
