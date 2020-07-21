package org.gbif.maps;

import javax.annotation.Nullable;
import org.gbif.occurrence.search.es.EsConfig;

/**
 * Application configuration with sensible defaults if applicable.
 */
public class TileServerConfiguration {

  private HBaseConfiguration hbase;

  private EsTileConfiguration esConfiguration;

  private Metastore metastore;

  public HBaseConfiguration getHbase() {
    return hbase;
  }

  public void setHbase(HBaseConfiguration hbase) {
    this.hbase = hbase;
  }

  public EsTileConfiguration getEsConfiguration() {
    return esConfiguration;
  }

  public void setEsConfiguration(EsTileConfiguration esConfiguration) {
    this.esConfiguration = esConfiguration;
  }

  public Metastore getMetastore() {
    return metastore;
  }

  public void setMetastore(@Nullable Metastore metastore) {
    this.metastore = metastore;
  }

  public static class Metastore {

    private String zookeeperQuorum;

    private String path;

    public String getZookeeperQuorum() {
      return zookeeperQuorum;
    }

    public String getPath() {
      return path;
    }

    public void setPath(String path) {
      this.path = path;
    }

    public void setZookeeperQuorum(String zookeeperQuorum) {
      this.zookeeperQuorum = zookeeperQuorum;
    }
  }

  public static class HBaseConfiguration  {

    private String zookeeperQuorum;

    private String tilesTableName;

    private String pointsTableName;

    private Integer tileSize;

    private Integer bufferSize;

    private Integer saltModulus;

    public String getZookeeperQuorum() {
      return zookeeperQuorum;
    }

    public void setZookeeperQuorum(String zookeeperQuorum) {
      this.zookeeperQuorum = zookeeperQuorum;
    }

    public String getTilesTableName() {
      return tilesTableName;
    }

    public void setTilesTableName(String tilesTableName) {
      this.tilesTableName = tilesTableName;
    }

    public String getPointsTableName() {
      return pointsTableName;
    }

    public void setPointsTableName(String pointsTableName) {
      this.pointsTableName = pointsTableName;
    }

    public Integer getTileSize() {
      return tileSize;
    }

    public void setTileSize(Integer tileSize) {
      this.tileSize = tileSize;
    }

    public Integer getBufferSize() {
      return bufferSize;
    }

    public void setBufferSize(Integer bufferSize) {
      this.bufferSize = bufferSize;
    }

    public Integer getSaltModulus() {
      return saltModulus;
    }

    public void setSaltModulus(Integer saltModulus) {
      this.saltModulus = saltModulus;
    }
  }

  public static class EsTileConfiguration {

    private EsConfig elasticsearch;

    private Integer tileSize;

    private Integer bufferSize;

    public EsConfig getElasticsearch() {
      return elasticsearch;
    }

    public void setElasticsearch(EsConfig elasticsearch) {
      this.elasticsearch = elasticsearch;
    }

    public Integer getTileSize() {
      return tileSize;
    }

    public void setTileSize(Integer tileSize) {
      this.tileSize = tileSize;
    }

    public Integer getBufferSize() {
      return bufferSize;
    }

    public void setBufferSize(Integer bufferSize) {
      this.bufferSize = bufferSize;
    }

  }

}
