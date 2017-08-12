package org.gbif.maps.common.meta;

/**
 * Suppliers of the MapMetastore implementations.
 */
public class Metastores {

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
}
