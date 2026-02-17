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
package org.gbif.maps.config;

import org.gbif.occurrence.search.es.EsConfig;

import java.util.Map;

import lombok.Data;

public class Config {

  @Data
  public static class Metastore {
    private String zookeeperQuorum;
    private String path;
  }

  @Data
  public static class HBaseConfiguration  {
    private String zookeeperQuorum;
    private String hbaseZnode;
    private String pointsTableName;
    private String tilesTableName;
    private Map<String, String> taxonomyTilesTableNames;
    private Integer tileSize;
    private Integer bufferSize;
    private Integer featuresBufferSize;
    private Integer saltModulusPoints;
    private Integer saltModulusTiles;
  }

  @Data
  public static class EsTileConfiguration {
    private EsConfig elasticsearch;
    private Integer tileSize;
    private Integer bufferSize;
    private boolean nestedIndex;
    private boolean enabled;
  }

}
