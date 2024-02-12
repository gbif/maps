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
package org.gbif.maps.workflow;

import java.io.IOException;
import java.net.URL;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.io.Resources;

import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;
import lombok.extern.slf4j.Slf4j;

@Data
@Builder
@Jacksonized
@Slf4j
public class MapConfiguration {

  private String appName;
  private String snapshotDirectory;
  private String sourceSubdirectory;
  private String targetDirectory;
  private String timestamp;
  private String mode;
  private String hiveDB;
  private int tilesThreshold;
  private int tileSize;
  private int maxZoom;
  private int tileBufferSize;
  private HBaseConfiguration hbase;
  private HdfsLockConfig hdfsLockConfig;

  @Data
  @Builder
  @Jacksonized
  static class HBaseConfiguration {
    // private String zkQuorum;
    // private String rootDir;
    private int keySaltModulus;
    private String tableName;
  }

  @Data
  @Builder
  @Jacksonized
  static class HdfsLockConfig {
    // private String zkConnectionString;
    private String namespace;
    private String lockingPath;
    private String lockName;
    private int sleepTimeMs;
    private int maxRetries;
  }

  /** E.g. pass in the filename relative to the classpath, e.g. "/dev.yml" */
  static MapConfiguration build(String filename) throws IOException {
    URL conf = Resources.getResource(filename);
    log.info("Reading from {}", conf);
    return new ObjectMapper(new YAMLFactory()).readValue(conf, MapConfiguration.class);
  }

  public String getFQTableName() {
    return String.format("%s_%s_%s", hbase.tableName, mode, timestamp);
  }

  public String getFQTargetDirectory() {
    return String.format("%s_%s_%s", targetDirectory, mode, timestamp);
  }
}
