package org.gbif.ws.discovery.conf;

import com.beust.jcommander.Parameter;
import com.google.common.base.Strings;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

/**
 * Configuration class of services hosted by container.
 * Each element of the configuration is self-explanatory with the JCommander annotation @Parameter.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ServiceConfiguration {

  @Parameter(names = "-httpPort", description = "Http port", required = true)
  private int httpPort;

  @Parameter(names = "-httpAdminPort", description = "Http administration port", required = true)
  private int httpAdminPort;

  @Parameter(names = "-zkHost",
             description = "Zookeeper ensemble to store the discover service information",
             required = true)
  private String zkHost;

  @Parameter(names = "-zkPath",
             description = "Zookeeper path to store the discovery service information",
             required = true)
  private String zkPath;

  @Parameter(names = "-stopSecret", description = "Secret/password to stop the server", required = true)
  private String stopSecret;

  @Parameter(names = "-timestamp", description = "Timestamp that identifies this service instance", required = false)
  private Long timestamp;

  @Parameter(names = "-externalPort",
             description = "External port mapping used if the httpPort is not reachable, e.g: Linux containers.",
             required = false)
  private Integer externalPort;

  @Parameter(names = "-externalAdminPort",
             description = "External port mapping used if the httpAdminPort is not reachable, e.g: Linux containers.",
             required = false)
  private Integer externalAdminPort;

  @Parameter(names = "-host",
             description = "Application server or control host that runs the service instance",
             required = false)
  private String host;

  @Parameter(names = "-containerName",
             description = "Container name, intended to store the Linux container name",
             required = false)
  private String containerName;

  @Parameter(names = "-conf", description = "Path to the configuration file", required = true)
  private String conf;

  public String getZkHost() {
    return zkHost;
  }

  public void setZkHost(String zkHost) {
    this.zkHost = zkHost;
  }

  public boolean isRunsInContainer() {
    return containerName != null;
  }

  public String getStopSecret() {
    return stopSecret;
  }

  public void setStopSecret(String stopSecret) {
    this.stopSecret = stopSecret;
  }

  public Integer getHttpPort() {
    return httpPort;
  }

  public void setHttpPort(int httpPort) {
    this.httpPort = httpPort;
  }

  public Integer getHttpAdminPort() {
    return httpAdminPort;
  }

  public void setHttpAdminPort(int httpAdminPort) {
    this.httpAdminPort = httpAdminPort;
  }

  public Long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Long timestamp) {
    this.timestamp = timestamp;
  }

  public String getZkPath() {
    return zkPath;
  }

  public void setZkPath(String zkPath) {
    this.zkPath = zkPath;
  }

  public Integer getExternalPort() {
    return externalPort;
  }

  public void setExternalPort(Integer externalPort) {
    this.externalPort = externalPort;
  }

  public Integer getExternalAdminPort() {
    return externalAdminPort;
  }

  public void setExternalAdminPort(Integer externalAdminPort) {
    this.externalAdminPort = externalAdminPort;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public String getContainerName() {
    return containerName;
  }

  public void setContainerName(String containerName) {
    this.containerName = containerName;
  }

  public String getConf() {
    return conf;
  }

  public void setConf(String conf) {
    this.conf = conf;
  }

  public boolean isDiscoverable() {
    return !Strings.isNullOrEmpty(zkHost) && !Strings.isNullOrEmpty(zkPath);
  }
}
