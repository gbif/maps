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
package org.gbif.maps.udf;

import org.gbif.maps.common.projection.*;

import java.io.Serializable;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import lombok.AllArgsConstructor;

/** Projects and returns the global X,Y coordinates for the given lat,lng. */
@AllArgsConstructor
public class ProjectUDF implements UDF4<Double, Double, String, Integer, Row>, Serializable {
  final int tileSize;

  public static void register(SparkSession spark, String name, int tileSize) {
    spark
        .udf()
        .register(
            name,
            new ProjectUDF(tileSize),
            DataTypes.createStructType(
                new StructField[] {
                  DataTypes.createStructField("x", DataTypes.LongType, false),
                  DataTypes.createStructField("y", DataTypes.LongType, false),
                }));
  }

  @Override
  public Row call(Double lat, Double lng, String epsg, Integer zoom) {
    TileProjection projection = Tiles.fromEPSG(epsg, tileSize);
    if (projection.isPlottable(lat, lng)) {
      Double2D globalXY = projection.toGlobalPixelXY(lat, lng, zoom);
      long x = Double.valueOf(globalXY.getX()).longValue();
      long y = Double.valueOf(globalXY.getY()).longValue();
      return RowFactory.create(Long.valueOf(x), Long.valueOf(y));
    }
    return null;
  }
}
