package org.gbif.maps.common.meta;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Suppliers of the MapMetastore implementations.
 */
public class Metastores {

  public static void main(String[] args) throws Exception {
    if (args.length == 2 || args.length == 4) {
      String zkQuorum = args[0];
      String zkPath = args[1];
      System.out.println("Reading ZK[" + zkQuorum + "], path[" + zkPath+ "]");
      MapMetastore meta = newZookeeperMapsMeta(zkQuorum, 1000, zkPath);
      System.out.println(meta.read());

      if (args.length == 4) {
        String tileTable = args[2];
        String pointTable = args[3];
        System.out.println("Update tileTable[" + tileTable + "], pointTable[" + pointTable + "]");
        meta.update(new MapTables(tileTable, pointTable));
        System.out.println("Done.");
      }
    } else {
      System.out.println("Usage (supply table names to issue update): Metastores zkQuorum path [pointTable tileTable]");
    }
  }

  /**
   * Creates a new ZK backed MapMetastore that will reflect external changes.
   * @param zkEnsemble For the ZK assemble
   * @param retryIntervalMs Number of msecs between retries on ZK issues (1000 is sensible value)
   * @param zkNodePath To monitor
   * @return The metastore
   * @throws Exception If the environment is not working
   */
  public static MapMetastore newZookeeperMapsMeta(String zkEnsemble, int retryIntervalMs, String zkNodePath)
    throws Exception {
    return new ZKMapMetastore(zkEnsemble, retryIntervalMs, zkNodePath);
  }

  /**
   * Return a static value MapMetastore
   * @param tileTable The table name with tile data
   * @param pointTable The table name with point data
   * @return The metastore
   */
  public static MapMetastore newStaticMapsMeta(String tileTable, String pointTable) {
    final MapTables data = new MapTables(tileTable, pointTable);
    return new MapMetastore() {
      @Override
      public MapTables read() {
        return data;
      }

      @Override
      public void update(MapTables meta) {
      }

      @Override
      public void close(){
      }
    };
  }

}
