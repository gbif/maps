package org.gbif.maps.config;

import lombok.Data;

import org.gbif.occurrence.search.es.EsConfig;

import java.util.Map;

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
