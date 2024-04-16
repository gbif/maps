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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF13;
import org.apache.spark.sql.types.DataTypes;

import com.google.common.collect.Sets;

import lombok.AllArgsConstructor;
import scala.collection.JavaConversions;
import scala.collection.mutable.WrappedArray;

/** Returns the map keys for the record. */
@AllArgsConstructor
public class MapKeysUDF
    implements UDF13<
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
            Integer,
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
      new HashMap<String, Integer>() {
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

  public String[] call(Row row) {
    return call(
        row.getAs("kingdomKey"),
        row.getAs("phylumKey"),
        row.getAs("classKey"),
        row.getAs("orderKey"),
        row.getAs("familyKey"),
        row.getAs("genusKey"),
        row.getAs("speciesKey"),
        row.getAs("taxonKey"),
        row.getAs("datasetKey"),
        row.getAs("publishingOrgKey"),
        row.getAs("countryCode"),
        row.getAs("publishingCountry"),
        row.getAs("networkKey"));
  }

  @Override
  public String[] call(
      Integer kingdomKey,
      Integer phylumKey,
      Integer classKey,
      Integer orderKey,
      Integer familyKey,
      Integer genusKey,
      Integer speciesKey,
      Integer taxonKey,
      String datasetKey,
      String publishingOrgKey,
      String countryCode,
      String publishingCountry,
      WrappedArray<String> networkKeys) {

    Set<String> keys = Sets.newHashSet();
    appendNonNull(keys, "ALL", 0);
    appendNonNull(keys, "TAXON", kingdomKey);
    appendNonNull(keys, "TAXON", phylumKey);
    appendNonNull(keys, "TAXON", classKey);
    appendNonNull(keys, "TAXON", orderKey);
    appendNonNull(keys, "TAXON", familyKey);
    appendNonNull(keys, "TAXON", genusKey);
    appendNonNull(keys, "TAXON", speciesKey);
    appendNonNull(keys, "TAXON", taxonKey);
    appendNonNull(keys, "DATASET", datasetKey);
    appendNonNull(keys, "PUBLISHER", publishingOrgKey);
    appendNonNull(keys, "COUNTRY", countryCode);
    appendNonNull(keys, "PUBLISHING_COUNTRY", publishingCountry);
    if (networkKeys != null && networkKeys.size() > 0) {
      for (String n : JavaConversions.seqAsJavaList(networkKeys)) {
        appendNonNull(keys, "NETWORK", n);
      }
    }

    if (!denyOrApproveKeys.isEmpty()) {
      Set<String> filtered =
          keys.stream()
              .filter(
                  s -> {
                    if (isApprove) return denyOrApproveKeys.contains(s);
                    else return !denyOrApproveKeys.contains(s);
                  })
              .collect(Collectors.toSet());
      return filtered.toArray(new String[filtered.size()]);
    }

    return keys.toArray(new String[keys.size()]);
  }

  public static void appendNonNull(Set<String> target, String prefix, Object l) {
    if (l != null) target.add(MAPS_TYPES.get(prefix) + ":" + l);
  }
}
