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
package org.gbif.maps.resource.udf;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

/**
 * Encode basisOfRecord and year into an int which improves performance during aggregations across
 * large datasets considerably.
 */
public class EncodeBorYearUDF implements UDF2<String, Integer, Integer>, Serializable {

  private static final Map<String, Integer> BOR_MAPPING = new HashMap<>();
  private static final Map<Integer, String> BOR_MAPPING_REVERSE = new HashMap<>();

  static {
    BOR_MAPPING.put("PRESERVED_SPECIMEN", 0);
    BOR_MAPPING.put("MACHINE_OBSERVATION", 1);
    BOR_MAPPING.put("OCCURRENCE", 2);
    BOR_MAPPING.put("HUMAN_OBSERVATION", 3);
    BOR_MAPPING.put("LIVING_SPECIMEN", 4);
    BOR_MAPPING.put("OBSERVATION", 5);
    BOR_MAPPING.put("MATERIAL_CITATION", 6);
    BOR_MAPPING.put("MATERIAL_SAMPLE", 7);
    BOR_MAPPING.put("FOSSIL_SPECIMEN", 8);

    for (Map.Entry<String, Integer> e : BOR_MAPPING.entrySet()) {
      BOR_MAPPING_REVERSE.put(e.getValue(), e.getKey());
    }
  }

  public static void register(SparkSession spark, String name) {
    spark.udf().register(name, new EncodeBorYearUDF(), DataTypes.IntegerType);
  }

  @Override
  public Integer call(String bor, Integer year) {
    return encode(bor, year);
  }

  static int encode(String bor, Integer year) {
    int b = BOR_MAPPING.get(bor);
    int y = year == null ? 0 : year;
    return (y * 100) + b;
  }

  public static int year(int encoded) {
    return Math.max(encoded / 100, 0); // null years are 0
  }

  public static String bor(int encoded) {
    return BOR_MAPPING_REVERSE.get(encoded % 100);
  }
}
