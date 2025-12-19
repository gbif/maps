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
package org.gbif.maps;

import org.gbif.occurrence.search.es.EsConfig;

import lombok.Data;

/**
 * Application configuration with sensible defaults if applicable.
 */
@Data
public class TileServerConfiguration {
  private HBaseConfiguration hbase;
  private EsTileConfiguration esOccurrenceConfiguration;
  private Metastore metastore;

  @Data
  public static class Metastore {
    private String zookeeperQuorum;
    private String path;
  }

  @Data
  public static class HBaseConfiguration  {
    private String zookeeperQuorum;
    private String hbaseZnode;
    private String tilesTableName;
    private String pointsTableName;
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
