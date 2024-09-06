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
package org.gbif.maps.common.meta;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Suppliers of the MapMetastore implementations.
 */
public class Metastores {
  private static final Logger LOG = LoggerFactory.getLogger(Metastores.class);

  public static void main(String[] args) throws Exception {
    if (args.length == 2 || args.length == 4) {
      String zkQuorum = args[0];
      String zkPath = args[1];
      LOG.info("Reading ZK[{}], path[{}]", zkQuorum, zkPath);
      try(MapMetastore meta = newZookeeperMapsMeta(zkQuorum, 1000, zkPath)) {
        LOG.info("{}", meta.read());

        if (args.length == 4) {
          String tileTable = args[2];
          String pointTable = args[3];
          LOG.info("Update tileTable[{}], pointTable[{}]", tileTable, pointTable);
          meta.update(new MapTables(tileTable, pointTable));
          LOG.info("Done.");
        }
      }
    } else {
      LOG.error("Usage (supply table names to issue update): Metastores zkQuorum path [pointTable tileTable]");
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
    LOG.info("Static tileTable[{}], pointTable[{}]", tileTable, pointTable);
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
