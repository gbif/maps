package org.gbif.ws.discovery.conf;

import com.google.common.base.Objects;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

/**
 * This class contains the information that is published in the discovery service registry (Zookeeper).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ServiceDetails {


  private static final String URL_FMT = "http://%s:%s/";

  // Maven settings
  private String groupId;
  private String artifactId;
  private String version;

  private ServiceConfiguration serviceConfiguration;

  private ServiceStatus status;

  public String getGroupId() {
    return groupId;
  }

  public void setGroupId(String groupId) {
    this.groupId = groupId;
  }

  public String getArtifactId() {
    return artifactId;
  }

  public void setArtifactId(String artifactId) {
    this.artifactId = artifactId;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public ServiceConfiguration getServiceConfiguration() {
    return serviceConfiguration;
  }

  public void setServiceConfiguration(ServiceConfiguration serviceConfiguration) {
    this.serviceConfiguration = serviceConfiguration;
  }

  public String getName() {
    return artifactId;
  }

  /**
   * Service status, this status is published in the service registry.
   */
  public ServiceStatus getStatus() {
    return status;
  }

  public void setStatus(ServiceStatus status) {
    this.status = status;
  }

  /**
   * The full service name contains the artifact and the version.
   */
  public String getFullName() {
    return artifactId + '-' + version;
  }

  public String getExternalUrl() {
    return String.format(URL_FMT,
                         serviceConfiguration.getHost(),
                         Objects.firstNonNull(serviceConfiguration.getExternalPort(),
                                              serviceConfiguration.getHttpPort()),
                                              "Unknown");
  }

}
