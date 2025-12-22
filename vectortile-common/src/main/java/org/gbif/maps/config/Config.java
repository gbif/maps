package org.gbif.maps.config;

import lombok.Data;

import org.gbif.occurrence.search.es.EsConfig;

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
    public enum SearchType { EVENT, OCCURRENCE}
    private EsConfig elasticsearch;
    private Integer tileSize;
    private Integer bufferSize;
    private SearchType type;
    private boolean nestedIndex;
    private boolean enabled;
  }

}
