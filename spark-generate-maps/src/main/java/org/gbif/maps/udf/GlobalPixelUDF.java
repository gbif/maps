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
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import lombok.AllArgsConstructor;

/** Converts a lat,lng into the global pixel space for a given projection and zoom level. */
@AllArgsConstructor
public class GlobalPixelUDF implements UDF3<Integer, Double, Double, Row>, Serializable {
  final String epsg;
  final int tileSize;

  public static void register(SparkSession spark, String name, String epsg, int tileSize) {
    spark
        .udf()
        .register(
            name,
            new GlobalPixelUDF(epsg, tileSize),
            DataTypes.createStructType(
                new StructField[] {
                  DataTypes.createStructField("x", DataTypes.IntegerType, false),
                  DataTypes.createStructField("y", DataTypes.IntegerType, false)
                }));
  }

  @Override
  public Row call(Integer zoom, Double lat, Double lng) {
    TileProjection projection = Tiles.fromEPSG(epsg, tileSize);
    if (projection.isPlottable(lat, lng)) {
      Double2D globalXY = projection.toGlobalPixelXY(lat, lng, zoom);
      int x = Double.valueOf(globalXY.getX()).intValue();
      int y = Double.valueOf(globalXY.getY()).intValue();
      return RowFactory.create(Integer.valueOf(x), Integer.valueOf(y));
    }
    return null;
  }
}
