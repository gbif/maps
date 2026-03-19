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

import java.util.UUID;

import lombok.extern.slf4j.Slf4j;

/** The driver for back-filling the map tables from Airflow. */
@Slf4j
public class Backfill {

  /** Expects configFile timestamp. */
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      throw new IllegalArgumentException("Expects configFile timestamp");
    }

    MapConfiguration config = MapConfiguration.build(args[0]);
    config.setTimestamp(args[1]);
    String snapshotName = UUID.randomUUID().toString();
    log.info("Creating snapshot {} {}", config.getSnapshotDirectory(), snapshotName);

    String mode = config.getMode(); // tiles or points
    MapBuilder mapBuilder =
        MapBuilder.builder()
            .hiveDB(config.getHiveDB())
            .hiveInputSuffix(mode + "_" + config.getTimestamp())
            .hbaseTable(config.getFQTableName())
            .targetDir(config.getFQTargetDirectory())
            .moduloPoints(config.getHbase().getKeySaltModulusPoints())
            .moduloTiles(config.getHbase().getKeySaltModulusTiles())
            .threshold(config.getTilesThreshold())
            .tileSize(config.getTileSize())
            .bufferSize(config.getTileBufferSize())
            .maxZoom(config.getMaxZoom())
            .buildPoints(mode.equalsIgnoreCase("points"))
            .buildTiles(mode.equalsIgnoreCase("tiles"))
            .buildNonTaxonTiles(config.isProcessNonChecklistTiles())
            .checklistsToTile(config.getChecklistsToProcess())
            .build();
    log.info("Launching map build with config: {}", mapBuilder);
    mapBuilder.run();
  }

}
