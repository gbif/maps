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

import org.gbif.maps.MapBuilder;
import lombok.extern.slf4j.Slf4j;

/** The driver for back-filling the map tables from Airflow. */
@Slf4j
public class Backfill {

  /** Expects [tiles,points] configFile [airflowProperties] */
  public static void main(String[] args) throws Exception {
    if (args.length != 3 && args.length != 4) {
      throw new IllegalArgumentException(
          "Expects [tiles,points] configFile timestamp [airflowProperties]");
    }

    MapConfiguration config = MapConfiguration.build(args[1]);
    config.setTimestamp(args[2]);
    config.setMode(args[0]);

    String mode = args[0].toLowerCase(); // tiles or points
    MapBuilder mapBuilder =
        MapBuilder.builder()
            .hiveDB(config.getHiveDB())
            .hiveInputSuffix(mode)
            .hbaseTable(config.getFQTableName())
            .targetDir(config.getFQTargetDirectory())
            .moduloPoints(config.getHbase().getKeySaltModulusPoints())
            .moduloTiles(config.getHbase().getKeySaltModulusTiles())
            .threshold(config.getTilesThreshold())
            .tileSize(config.getTileSize())
            .bufferSize(config.getTileBufferSize())
            .maxZoom(config.getMaxZoom())
            .projectionParallelism(config.getProjectionParallelism())
            .buildPoints(mode.equalsIgnoreCase("points"))
            .buildTiles(mode.equalsIgnoreCase("tiles"))
            .build();
    log.info("Launching map build with config: {}", mapBuilder);
    mapBuilder.run();
  }
}
