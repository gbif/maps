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
import java.util.List;

import org.apache.spark.sql.Row;

import lombok.AllArgsConstructor;

import static org.gbif.maps.io.PointFeature.*;
import static org.gbif.maps.io.PointFeature.PointFeatures.*;

/** Generates a GBIF protobug tile from the structured input. */
@AllArgsConstructor
public class ProtobufTiles implements Serializable {
  public static byte[] generate(Row row) {
    List<Row> tileData = row.getList(row.fieldIndex("features"));
    PointFeatures.Builder tile = PointFeatures.newBuilder();
    Feature.Builder feature = Feature.newBuilder();
    tileData.stream()
        .forEach(
            f -> {
              String bor = EncodeBorYearUDF.bor(f.getAs("borYear"));
              Integer year = EncodeBorYearUDF.year(f.getAs("borYear"));
              year = year == null ? 0 : year;

              feature.setLatitude(f.getAs("lat"));
              feature.setLongitude(f.getAs("lng"));
              feature.setBasisOfRecord(Feature.BasisOfRecord.valueOf(bor));
              feature.setYear(year);

              tile.addFeatures(feature.build());
              feature.clear();
            });
    return tile.build().toByteArray();
  }
}
