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

import java.util.Map;

/**
 * Suppliers of the MapMetastore implementations.
 */
public class Metastores {
  private static final Logger LOG = LoggerFactory.getLogger(Metastores.class);

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
   * Return a static value MapMetastore.
   */
  public static MapMetastore newStaticMapsMeta(String pointTable, String tileTable, Map<String, String> taxonomyTileTables) {
    LOG.info("Static pointTable[{}], tileTable[{}], taxonomyTileTables[{}] ", pointTable, tileTable, taxonomyTileTables);
    final MapTables data = new MapTables(pointTable, tileTable, taxonomyTileTables);
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
