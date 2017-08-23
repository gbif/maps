package org.gbif.maps.workflow;

import org.gbif.maps.common.hbase.ModulusSalt;
import org.gbif.maps.common.meta.MapMetastore;
import org.gbif.maps.common.meta.MapTables;
import org.gbif.maps.common.meta.Metastores;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.v2.app.MRAppMaster;

/**
 * Finalising the backfill involves the following:
 * <ol>
 *   <li>Bulkloading the HFiles into the target table</li>
 *   <li>Updating the Maps metastore table (ZK)</li>
 *   <li>Deleting the snapshot table</li>
 *   <li>Deleting the intermediate folders</li>
 * </ol>
 */
public class FinaliseBackfill {
  // TODO: Move this to configuration
  private final static String[] PROJECTIONS = {
    "EPSG_3857", "EPSG_4326", "EPSG_3575", "EPSG_3031"
  };

  // TODO: Move this to configuration
  private final static int MAX_ZOOM = 16;


  public static void main(String[] args) throws Exception {
    WorkflowParams params = WorkflowParams.buildFromOozie(args[0]);
    System.out.println(params.toString());

    loadTable(params); // load HBase (throws exception on error)
    updateMeta(params); // update the metastore in ZK
    cleanup(params);
  }

  /**
   * Updates the tile or point table registration in the metadata, depending on the mode in which we are running.
   */
  private static void updateMeta(WorkflowParams params) throws Exception {
    // 1 sec retries
    MapMetastore metastore = Metastores.newZookeeperMapsMeta(params.getZkQuorum(), 1000, params.getZkMetaDataPath());

    MapTables meta = metastore.read(); // we update any existing values

    // NOTE: there is the possibility of a race condition here if 2 instances are updating different modes
    // simulataneously
    if ("points".equalsIgnoreCase(params.getMode())) {
      MapTables newMeta = new MapTables((meta == null) ? null : meta.getTileTable(), params.getTargetTable());
      System.out.println("Updating metadata with: " + newMeta);
      metastore.update(newMeta);

    } else {
      MapTables newMeta = new MapTables(params.getTargetTable(), (meta == null) ? null : meta.getPointTable());
      System.out.println("Updating metadata with: " + newMeta);
      metastore.update(newMeta);
    }
  }

  private static void loadTable(WorkflowParams params) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.ZOOKEEPER_QUORUM, params.getZkQuorum());

    try (
      Connection connection = ConnectionFactory.createConnection(conf);
      HTable hTable = new HTable(conf, params.getTargetTable());
    ) {
      LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);

      // bulkload requires files to be in hbase ownership
      FsShell shell=new FsShell(conf);
      try {
        System.out.println("Executing chown -R hbase:hbase for " + params.getTargetDirectory());
        shell.run(new String[]{"-chown","-R","hbase:hbase", params.getTargetDirectory()});
      } catch (Exception e) {
        throw new IOException("Unable to modify FS ownership to hbase", e);
      }

      if ("points".equalsIgnoreCase(params.getMode())) {
        Path hfiles = new Path(params.getTargetDirectory(), new Path("points"));
        System.out.println("Loading HBase table[" + params.getTargetTable()+ "] from [" + hfiles + "]");
        loader.doBulkLoad(hfiles, hTable);

      } else {
        for (String projection : PROJECTIONS) {
          for (int zoom=0; zoom<=MAX_ZOOM; zoom++) {
            Path hfiles = new Path(params.getTargetDirectory(), new Path("tiles", new Path(projection, "z" + zoom)));
            System.out.println("Zoom[" + zoom +"] Loading HBase table[" + params.getTargetTable()+ "] from [" + hfiles + "]");
            loader.doBulkLoad(hfiles, hTable);
          }
        }
      }
    }
  }

  /**
   * Deletes the snapshot and old tables whereby we keep the 2 latest tables only.
   */
  private static void cleanup(WorkflowParams params) throws Exception {
    try {
      System.out.println("Connecting to HBase");
      Configuration conf = HBaseConfiguration.create();
      conf.set(HConstants.ZOOKEEPER_QUORUM, params.getZkQuorum());
      try (
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
      ) {

        // remove the snapshot created in this workflow only
        System.out.println("Deleting HBase snapshot[" + params.getSnapshotTable()+ "]");
        //admin.deleteSnapshot(params.getSnapshotTable());

        // remove all but the last 2 tables
        // table names are suffixed with a timestamp e.g. prod_d_maps_points_20180616_1320
        String tablesPattern = params.getTargetTablePrefix() + "_" + params.getMode() + "_d{8}_d{4}";
        TableName[] tables = admin.listTableNames(tablesPattern);
        Arrays.sort(tables, new Comparator<TableName>() {  // TableName does not order lexigraphically by default
          @Override
          public int compare(TableName o1, TableName o2) {
            return o1.getNameAsString().compareTo(o2.getNamespaceAsString());
          }
        });
        for (int i=0; i<tables.length-2; i++) {
          // Defensive coding: safeguard against corrupt table names
          if (!params.getTargetTable().equalsIgnoreCase(tables[i].getNamespaceAsString())) {
            System.out.println("Disabling HBase table[" + tables[i].getNameAsString() + "]");
            //admin.disableTable(tables[i]);
            System.out.println("Deleting HBase table[" + tables[i].getNameAsString() + "]");
            //admin.deleteTable(tables[i]);
          }
        }
      }
    } catch (IOException e) {
      System.err.println("Unable to clean HBase tables");
      e.printStackTrace();
      throw e; // deliberate log and throw to keep logs together
    }

  }
}
