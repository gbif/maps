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
package org.gbif.maps;

import org.gbif.maps.udf.EncodeBorYearUDF;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Row;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

import lombok.AllArgsConstructor;
import no.ecc.vectortile.VectorTileEncoder;

/** Generates a vector tile from the structured input. */
@AllArgsConstructor
public class VectorTiles implements Serializable {
  private int tileSize;
  private int bufferSize;
  private static GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  public byte[] generate(List<Row> tileData) {
    VectorTileEncoder encoder = new VectorTileEncoder(tileSize, bufferSize, false);
    for (Row pixel : tileData) {
      int x = pixel.getAs("x");
      int y = pixel.getAs("y");

      /*
            // 4326 is alarming slow: diagnostics to remove
            Preconditions.checkArgument(
                x >= -bufferSize && x <= tileSize + bufferSize,
                "Pixel x cannot be " + x + " tileSize=" + tileSize + " bufferSize=" + bufferSize);
            Preconditions.checkArgument(
                y >= -bufferSize && y <= tileSize + bufferSize,
                "Pixel y cannot be " + y + " tileSize=" + tileSize + " bufferSize=" + bufferSize);
      */

      Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(x, y));
      List<Row> features = pixel.getList(pixel.fieldIndex("features"));

      // Restructures our encoded list of borYear:count to the target structure in the MVT
      Map<String, Map<String, Long>> target = new HashMap<>();
      /*
      Preconditions.checkArgument(
          features.size() < 1000, "Features are too big at " + features.size());
      */
      for (Row encoded : features) {
        String bor = EncodeBorYearUDF.bor(encoded.getAs("borYear"));
        Integer year = EncodeBorYearUDF.year(encoded.getAs("borYear"));

        /*
        // 4326 is alarming slow: diagnostics to remove
        Preconditions.checkArgument(
            year == null || year == 0 || (year >= 1500 && year < 2024), "Year cannot be " + year);
         */

        long count = encoded.getAs("occCount");
        Map<String, Long> yearCounts = target.getOrDefault(bor, new HashMap<>());
        yearCounts.put(String.valueOf(year), count); // TODO: what are nulls meant to be?
        if (!target.containsKey(bor)) target.put(bor, yearCounts);
      }

      for (String bor : target.keySet()) {
        encoder.addFeature(bor, target.get(bor), point);
      }
    }
    return encoder.encode();
  }
}
