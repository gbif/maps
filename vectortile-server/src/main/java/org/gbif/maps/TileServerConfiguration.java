package org.gbif.maps;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import org.gbif.ws.discovery.conf.ServiceConfiguration;

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

  @Valid
  @Nullable
  private Metastore metastore;

  @Valid
  @NotNull
  private ServiceConfiguration service;

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

  @JsonProperty
  public ServiceConfiguration getService() { return service; }

  @JsonProperty
  public void setService(ServiceConfiguration service) { this.service = service; }

  @JsonProperty
  public Metastore getMetastore() {
    return metastore;
  }

  @JsonProperty
  public void setMetastore(@Nullable Metastore metastore) {
    this.metastore = metastore;
  }

  public static class Metastore extends Configuration {
    @Valid
    @NotNull
    private String zookeeperQuorum;

    @Valid
    @NotNull
    private String path;

    @JsonProperty
    public String getZookeeperQuorum() {
      return zookeeperQuorum;
    }

    @JsonProperty
    public String getPath() {
      return path;
    }

    @JsonProperty
    public void setPath(String path) {
      this.path = path;
    }

    @JsonProperty
    public void setZookeeperQuorum(String zookeeperQuorum) {
      this.zookeeperQuorum = zookeeperQuorum;
    }
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

    @Valid
    @NotNull
    private Integer saltModulus;

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

    @JsonProperty
    public Integer getSaltModulus() {
      return saltModulus;
    }
    @JsonProperty
    public void setSaltModulus(Integer saltModulus) {
      this.saltModulus = saltModulus;
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
