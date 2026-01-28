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

import java.io.Serializable;
import java.util.*;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF6;
import org.apache.spark.sql.types.DataTypes;

import com.google.common.collect.Sets;

import lombok.AllArgsConstructor;
import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;

/** Returns the map keys for the record. */
@AllArgsConstructor
public class MapKeysUDF
    implements UDF6<
            scala.collection.Map<String, WrappedArray<String>>,
            String,
            String,
            String,
            String,
            WrappedArray<String>,
            String[]>,
        Serializable {

  private Set<String> denyOrApproveKeys;
  private boolean isApprove;

  public static void register(SparkSession spark, String name) {
    register(spark, name, new HashSet<>(), true);
  }

  public static void register(
      SparkSession spark, String name, Set<String> denyOrApproveKeys, boolean isApprove) {
    MapKeysUDF udf = new MapKeysUDF(denyOrApproveKeys, isApprove);
    spark.udf().register(name, udf, DataTypes.createArrayType(DataTypes.StringType));
  }

  // Maintain backwards compatible keys
  private static final Map<String, Integer> MAPS_TYPES =
      new HashMap<>() {
        {
          put("ALL", 0);
          put("TAXON", 1);
          put("DATASET", 2);
          put("PUBLISHER", 3);
          put("COUNTRY", 4);
          put("PUBLISHING_COUNTRY", 5);
          put("NETWORK", 6);
        }
      };

  @Override
  public String[] call(
      scala.collection.Map<String, WrappedArray<String>> classifications,
      String datasetKey,
      String publishingOrgKey,
      String countryCode,
      String publishingCountry,
      WrappedArray<String> networkKeys) {

    Set<String> keys = Sets.newHashSet();

    appendNonNull(keys, "ALL", 0);
    appendNonNull(keys, "DATASET", datasetKey);
    appendNonNull(keys, "PUBLISHER", publishingOrgKey);
    appendNonNull(keys, "COUNTRY", countryCode);
    appendNonNull(keys, "PUBLISHING_COUNTRY", publishingCountry);

    if (networkKeys != null && !networkKeys.isEmpty()) {
      for (String n : JavaConverters.seqAsJavaList(networkKeys)) {
        appendNonNull(keys, "NETWORK", n);
      }
    }

    if (classifications != null) {
      java.util.Map<String, WrappedArray<String>> javaMap =
          JavaConverters.mapAsJavaMap(classifications);

      // for each classification, encode keys as "<classificationKey>|<taxonID>"
      for (Map.Entry<String, WrappedArray<String>> entry : javaMap.entrySet()) {
        String checklistKey = entry.getKey();
        List<String> taxa = JavaConverters.seqAsJavaList(entry.getValue());

        if (taxa == null) {
          continue;
        }

        for (String taxonId : taxa) {
          if (taxonId != null) {
            // encode the taxon key as "checklist|id"
            appendNonNull(keys, "TAXON", checklistKey + "|" + taxonId);
          }
        }
      }
    }

    if (!denyOrApproveKeys.isEmpty()) {
      return keys.stream()
          .filter(
              s -> {
                if (isApprove) return denyOrApproveKeys.contains(s);
                else return !denyOrApproveKeys.contains(s);
              })
          .toArray(String[]::new);
    }

    return keys.toArray(new String[0]);
  }

  public static void appendNonNull(Set<String> target, String prefix, Object l) {
    if (l != null) {
      Integer type = MAPS_TYPES.get(prefix);
      if (type != null) {
        target.add(type + ":" + l);
      }
    }
  }
}
