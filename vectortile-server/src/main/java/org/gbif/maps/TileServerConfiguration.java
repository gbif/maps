package org.gbif.maps;

import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import ch.qos.logback.core.joran.spi.JoranException;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import io.dropwizard.logging.LoggingUtil;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.util.ContextInitializer;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.dropwizard.logging.LoggingFactory;
import org.gbif.discovery.conf.ServiceConfiguration;

/**
 * Application configuration with sensible defaults if applicable.
 */
public class  TileServerConfiguration extends Configuration {
  private static final LogbackAutoConfigLoggingFactory LOGGING_FACTORY = new LogbackAutoConfigLoggingFactory();

  @Valid
  @NotNull
  private HBaseConfiguration hbase;

  @Valid
  @NotNull
  private EsConfiguration esConfig;

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
  public EsConfiguration getEsConfig() {
    return esConfig;
  }

  @JsonProperty
  public void setEsConfig(EsConfiguration esConfig) {
    this.esConfig = esConfig;
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
    private String tilesTableName;

    @Valid
    @NotNull
    private String pointsTableName;

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
    public String getTilesTableName() {
      return tilesTableName;
    }

    @JsonProperty
    public void setTilesTableName(String tilesTableName) {
      this.tilesTableName = tilesTableName;
    }

    @JsonProperty
    public String getPointsTableName() {
      return pointsTableName;
    }

    @JsonProperty
    public void setPointsTableName(String pointsTableName) {
      this.pointsTableName = pointsTableName;
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

  public static class EsConfiguration extends Configuration {

    @Valid
    @NotNull
    private String[] hosts;

    @Valid
    @NotNull
    private String index;

    @Valid
    @NotNull
    private Integer tileSize;

    @Valid
    @NotNull
    private Integer bufferSize;

    @Valid
    @NotNull
    private Integer connectTimeout = 3000;

    @Valid
    @NotNull
    private Integer socketTimeout = 60000;

    @Valid
    @NotNull
    private Integer maxRetryTimeoutMillis = 60000;

    @JsonProperty
    public String[] getHosts() {
      return hosts;
    }

    public void setHosts(String[] hosts) {
      this.hosts = hosts;
    }

    @JsonProperty
    public String getIndex() {
      return index;
    }

    public void setIndex(String index) {
      this.index = index;
    }

    @JsonProperty
    public Integer getTileSize() {
      return tileSize;
    }

    public void setTileSize(Integer tileSize) {
      this.tileSize = tileSize;
    }

    @JsonProperty
    public Integer getBufferSize() {
      return bufferSize;
    }

    public void setBufferSize(Integer bufferSize) {
      this.bufferSize = bufferSize;
    }

    @JsonProperty
    public Integer getConnectTimeout() {
      return connectTimeout;
    }

    public void setConnectTimeout(Integer connectTimeout) {
      this.connectTimeout = connectTimeout;
    }

    @JsonProperty
    public Integer getSocketTimeout() {
      return socketTimeout;
    }

    public void setSocketTimeout(Integer socketTimeout) {
      this.socketTimeout = socketTimeout;
    }

    @JsonProperty
    public Integer getMaxRetryTimeoutMillis() {
      return maxRetryTimeoutMillis;
    }

    public void setMaxRetryTimeoutMillis(Integer maxRetryTimeoutMillis) {
      this.maxRetryTimeoutMillis = maxRetryTimeoutMillis;
    }
  }

  @Override
  public LoggingFactory getLoggingFactory() {
    return LOGGING_FACTORY;
  }

  /**
   * https://github.com/dropwizard/dropwizard/issues/1567
   * Override getLoggingFactory for your configuration
   */
  private static class LogbackAutoConfigLoggingFactory implements LoggingFactory {

    @JsonIgnore
    private LoggerContext loggerContext;
    @JsonIgnore
    private final ContextInitializer contextInitializer;

    public LogbackAutoConfigLoggingFactory() {
      loggerContext = LoggingUtil.getLoggerContext();
      contextInitializer = new ContextInitializer(loggerContext);
    }

    @Override
    public void configure(MetricRegistry metricRegistry, String name) {
      try {
        loggerContext.reset();
        contextInitializer.autoConfig();
      } catch (JoranException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void stop() {
      loggerContext.stop();
    }
  }
}
