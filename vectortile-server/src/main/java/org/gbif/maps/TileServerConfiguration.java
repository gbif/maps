package org.gbif.maps;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import io.dropwizard.client.HttpClientConfiguration;

/**
 * Application configuration with sensible defaults if applicable.
 */
public class TileServerConfiguration extends Configuration {
  @Valid
  @NotNull
  private HttpClientConfiguration httpClient = new HttpClientConfiguration();

  @JsonProperty("httpClient")
  public HttpClientConfiguration getHttpClientConfiguration() {
    return httpClient;
  }

  @JsonProperty("httpClient")
  public void setHttpClientConfiguration(HttpClientConfiguration httpClient) {
    this.httpClient = httpClient;
  }

/*

  @NotEmpty
  private String hbaseZookeeperQuorum;
  private int hbaseZookeeperClientPort;
  @NotEmpty
  private String hbaseTableName;

  @JsonProperty
  public String getHbaseZookeeperQuorum() {
    return hbaseZookeeperQuorum;
  }

  @JsonProperty
  public void setHbaseZookeeperQuorum(String hbaseZookeeperQuorum) {
    this.hbaseZookeeperQuorum = hbaseZookeeperQuorum;
  }

  @JsonProperty
  public int getHbaseZookeeperClientPort() {
    return hbaseZookeeperClientPort;
  }

  @JsonProperty
  public void setHbaseZookeeperClientPort(int hbaseZookeeperClientPort) {
    this.hbaseZookeeperClientPort = hbaseZookeeperClientPort;
  }

  @JsonProperty
  public String getHbaseTableName() {
    return hbaseTableName;
  }

  @JsonProperty
  public void setHbaseTableName(String hbaseTableName) {
    this.hbaseTableName = hbaseTableName;
  }

  */
}
