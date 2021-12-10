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
package org.gbif.maps.utils;

import org.gbif.maps.resource.HBaseMaps;
import org.gbif.maps.resource.Params;

import java.io.File;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import com.google.common.io.Files;

/**
 * This utility exports a raw tile without modifying it and save it to a file.
 * This is useful to extract tiles from HBase for diagnostic reasons.
 * Usage (params is the same format as a web request:
 * <pre>
 *   ExportRawTile zk tableName saltModulus srs zoom x y params targetFile
 *   ExportRawTile c5zk1.gbif.org prod_h_maps_tiles_20211208_1900 100 EPSG_4326 3 8 2 publishingCountry=FR /tmp/publishingCountry-FR-3-8-2.mvt
 * </pre>
 *
 */
public class ExportRawTile {
  public static void main(String[] args) {

    try {
      String zk = args[0];
      String tableName = args[1];
      int salt = Integer.parseInt(args[2]);
      String srs = args[3];
      int z = Integer.parseInt(args[4]);
      long x = Long.parseLong(args[5]);
      long y = Long.parseLong(args[6]);

      // export the map key from the provided "request parameters"
      Map<String, String[]> params = paramsFromString(args[7]);
      String mapKey = Params.mapKeys(params)[0];

      File targetFile = new File(args[8]);

      Configuration conf = HBaseConfiguration.create();
      conf.set("hbase.zookeeper.quorum", zk);
      HBaseMaps maps = null;
        maps = new HBaseMaps(conf, tableName, salt);
      Optional<byte[]> tile = maps.getTile(mapKey, srs, z, x, y);
      if (tile.isPresent()) {
        Files.write(tile.get(), targetFile);
        System.out.println("File written");
      } else {
        System.out.println("No tile found!");
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  static  Map<String, String[]> paramsFromString(String s) {
    Map<String, String[]> params = Arrays.stream(s.split("&"))
      .map(s1 -> s1.split("="))
      .collect(Collectors.toMap(s1 -> s1[0], s1 -> new String[]{s1[1]}));
    return params;
  }
}
