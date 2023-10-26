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

import org.gbif.maps.common.hbase.ModulusSalt;

import java.io.Serializable;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.types.DataTypes;

import lombok.AllArgsConstructor;

/** Generates a salted HBase key for the given map tile coordinates. */
public class HBaseKeyUDF implements Serializable {

  public static void registerTileKey(SparkSession spark, String name, ModulusSalt salter) {
    spark.udf().register(name, new HBaseTileKey(salter), DataTypes.StringType);
  }

  public static void registerPointKey(SparkSession spark, String name, ModulusSalt salter) {
    spark.udf().register(name, new HBasePointKey(salter), DataTypes.StringType);
  }

  /** Generates keys for the tile pyramid table */
  @AllArgsConstructor
  static class HBaseTileKey
      implements UDF4<String, Integer, Integer, Integer, String>, Serializable {
    final ModulusSalt salter;

    @Override
    public String call(String mapKey, Integer z, Integer x, Integer y) {
      return salter.saltToString(String.format("%s:%d:%d:%d", mapKey, z, x, y));
    }
  }

  /** Generates keys for the point tile table */
  @AllArgsConstructor
  static class HBasePointKey implements UDF1<String, String>, Serializable {
    final ModulusSalt salter;

    @Override
    public String call(String mapKey) {
      return salter.saltToString(mapKey);
    }
  }
}
