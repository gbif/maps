package org.gbif.maps;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;

/**
 * Application configuration with sensible defaults if applicable.
 */
public class TileServerConfiguration extends Configuration {
  @Valid
  @NotNull
  private HBaseConfiguration hbase;

  @Valid
  @NotNull
  private SolrConfiguration solr;

  @JsonProperty
  public HBaseConfiguration getHbase() {
    return hbase;
  }

  @JsonProperty
  public void setHbase(HBaseConfiguration hbase) {
    this.hbase = hbase;
  }

  @JsonProperty
  public SolrConfiguration getSolr() {
    return solr;
  }

  @JsonProperty
  public void setSolr(SolrConfiguration solr) {
    this.solr = solr;
  }

  public static class HBaseConfiguration extends Configuration {
    @Valid
    @NotNull
    private String zookeeperQuorum;

    @Valid
    @NotNull
    private String tableName;

    @Valid
    @NotNull
    private Integer tileSize;

    @Valid
    @NotNull
    private Integer bufferSize;


    @JsonProperty
    public String getZookeeperQuorum() {
      return zookeeperQuorum;
    }

    @JsonProperty
    public void setZookeeperQuorum(String zookeeperQuorum) {
      this.zookeeperQuorum = zookeeperQuorum;
    }

    @JsonProperty
    public String getTableName() {
      return tableName;
    }

    @JsonProperty
    public void setTableName(String tableName) {
      this.tableName = tableName;
    }

    @JsonProperty
    public Integer getTileSize() {
      return tileSize;
    }

    @JsonProperty
    public void setTileSize(Integer tileSize) {
      this.tileSize = tileSize;
    }

    @JsonProperty
    public Integer getBufferSize() {
      return bufferSize;
    }

    @JsonProperty
    public void setBufferSize(Integer bufferSize) {
      this.bufferSize = bufferSize;
    }
  }

  public static class SolrConfiguration extends Configuration {
    @Valid
    @NotNull
    private String zookeeperQuorum;

    @Valid
    @NotNull
    private String defaultCollection;

    @Valid
    @NotNull
    private String requestHandler;

    @Valid
    @NotNull
    private Integer tileSize;

    @Valid
    @NotNull
    private Integer bufferSize;

    @JsonProperty
    public String getZookeeperQuorum() {
      return zookeeperQuorum;
    }

    @JsonProperty
    public void setZookeeperQuorum(String zookeeperQuorum) {
      this.zookeeperQuorum = zookeeperQuorum;
    }

    @JsonProperty
    public String getDefaultCollection() {
      return defaultCollection;
    }

    @JsonProperty
    public void setDefaultCollection(String defaultCollection) {
      this.defaultCollection = defaultCollection;
    }

    @JsonProperty
    public Integer getTileSize() {
      return tileSize;
    }

    @JsonProperty
    public void setTileSize(Integer tileSize) {
      this.tileSize = tileSize;
    }

    @JsonProperty
    public Integer getBufferSize() {
      return bufferSize;
    }

    @JsonProperty
    public void setBufferSize(Integer bufferSize) {
      this.bufferSize = bufferSize;
    }

    @JsonProperty
    public String getRequestHandler() {
      return requestHandler;
    }

    @JsonProperty
    public void setRequestHandler(String requestHandler) {
      this.requestHandler = requestHandler;
    }
  }
}
