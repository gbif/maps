package org.gbif.maps.workflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

import java.io.IOException;

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
  }


  private static void loadTable(WorkflowParams params) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.ZOOKEEPER_QUORUM, params.getZkQuorum());

    try (
      HTable hTable = new HTable(conf, params.getTargetTable())
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

}
