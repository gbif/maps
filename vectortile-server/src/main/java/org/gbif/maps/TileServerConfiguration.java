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

  private ClickhouseConfiguration clickhouse;

  private EsTileConfiguration esOccurrenceConfiguration;

  private EsTileConfiguration esEventConfiguration;

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

    private Integer saltModulus;

  }

  @Data
  public static class ClickhouseConfiguration  {

    private String endpoint;

    private String database;

    private String username;

    private String password;

    private Boolean enableConnectionPool;

    private Integer connectTimeout;

    private Integer maxConnections;

    private Integer maxRetries;

    private Integer socketTimeout;

  }

  @Data
  public static class EsTileConfiguration {

    public enum SearchType { EVENT, OCCURRENCE}

    private EsConfig elasticsearch;

    private Integer tileSize;

    private Integer bufferSize;

    private SearchType type;

    private boolean nestedIndex;

    private boolean enabled;

  }

}
