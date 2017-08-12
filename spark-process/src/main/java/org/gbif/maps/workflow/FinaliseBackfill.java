package org.gbif.maps.workflow;

import org.gbif.maps.common.hbase.ModulusSalt;
import org.gbif.maps.common.meta.MapMetastore;
import org.gbif.maps.common.meta.MapTables;
import org.gbif.maps.common.meta.Metastores;

import java.io.IOException;
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
  private final static String[] PROJECTIONS = {
    "EPSG_3857", "EPSG_4326", "EPSG_3575", "EPSG_3031"
  };

  public static void main(String[] args) throws Exception {
    WorkflowParams params = WorkflowParams.buildFromOozie(args[0]);
    System.out.println(params.toString());

    loadTable(params); // load HBase (throws exception on error)
    updateMeta(params); // update the metastore in ZK
    // TODO: cleanup tables
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
      metastore.update(new MapTables(null, params.getTargetTable()));

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
          Path hfiles = new Path(params.getTargetDirectory(), new Path("tiles", new Path(projection)));
          System.out.println("Loading HBase table[" + params.getTargetTable()+ "] from [" + hfiles + "]");
          loader.doBulkLoad(hfiles, hTable);
        }
      }
    }
  }
}
