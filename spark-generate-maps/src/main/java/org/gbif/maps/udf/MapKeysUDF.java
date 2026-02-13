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
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF5;
import org.apache.spark.sql.api.java.UDF6;
import org.apache.spark.sql.types.DataTypes;

import lombok.AllArgsConstructor;
import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;

/** A collection of UDFs suitable for generating map keys. */
@AllArgsConstructor
public class MapKeysUDF {

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

  /** Register a UDF that will generate all possible keys for a record. */
  public static void registerAllKeysUDF(SparkSession spark, String name) {
    registerAllKeysUDF(spark, name, new TreeSet<>(), true);
  }

  /**
   * Register a UDF that will generate all possible keys for a record filtered to those either
   * within or not within the options provided.
   */
  public static void registerAllKeysUDF(
      SparkSession spark, String name, TreeSet<String> denyOrApproveKeys, boolean isApprove) {
    spark
        .udf()
        .register(
            name,
            new AllKeysUDF(denyOrApproveKeys, isApprove),
            DataTypes.createArrayType(DataTypes.StringType));
  }

  /**
   * Registers a UDF that generates dictionary-encoded map keys for taxa in the classification,
   * restricted to values in the given dictionary.
   */
  public static void registerTaxonKeysUDF(
      SparkSession spark, String name, Map<String, Integer> dictionary) {
    spark
        .udf()
        .register(
            name,
            new TaxonMapKeysUDF(dictionary),
            DataTypes.createArrayType(DataTypes.IntegerType));
  }

  /**
   * Register a UDF that generates dictionary-encoded map keys for non-taxa fields, restricted to
   * values in the given dictionary.
   */
  public static void registerNonTaxonMapKeysUDF(
      SparkSession spark, String name, Map<String, Integer> dictionary) {
    spark
        .udf()
        .register(
            name,
            new NonTaxonMapKeysUDF(dictionary),
            DataTypes.createArrayType(DataTypes.IntegerType));
  }

  /**
   * Extracts all possible keys from the source record, optionally filtered to those either within,
   * or not within, a provided set of options.
   */
  @AllArgsConstructor
  public static class AllKeysUDF
      implements UDF6<
              scala.collection.Map<String, WrappedArray<String>>,
              String,
              String,
              String,
              String,
              WrappedArray<String>,
              String[]>,
          Serializable {
    private TreeSet<String> denyOrApproveKeys;
    private boolean isApprove;

    @Override
    public String[] call(
        scala.collection.Map<String, WrappedArray<String>> classifications,
        String datasetKey,
        String publishingOrgKey,
        String countryCode,
        String publishingCountry,
        WrappedArray<String> networkKeys) {

      Set<String> keys = new TreeSet<>(); // sorted, as we group by
      encodeToMapKey(
          keys, datasetKey, publishingOrgKey, countryCode, publishingCountry, networkKeys);

      // for each classification, encode keys as "...<classificationKey>|<taxonID>"
      if (classifications != null) {
        Map<String, WrappedArray<String>> javaMap = JavaConverters.mapAsJavaMap(classifications);

        for (Map.Entry<String, WrappedArray<String>> entry : javaMap.entrySet()) {
          encodeClassificationToMapKeys(keys, entry.getValue(), entry.getKey());
        }
      }

      // Optionally filter include either those denied or allowed
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
  }

  /**
   * Generates map keys for the taxa in the classification and checklist provided. The keys will be
   * created in the form "1:<checklistKey>|<taxoiD" but will then be dictionary encoded using the
   * given dictionary. Any generated key not found in the dictionary will be dropped.
   */
  @AllArgsConstructor
  public static class TaxonMapKeysUDF
      implements UDF2<WrappedArray<String>, String, Integer[]>, Serializable {

    private Map<String, Integer> dictionary;

    @Override
    public Integer[] call(WrappedArray<String> classification, String checklistKey) {
      Set<String> keys = new TreeSet<>(); // sorted, as we group by
      encodeClassificationToMapKeys(keys, classification, checklistKey);

      if (classification != null && !classification.isEmpty()) {
        for (String n : JavaConverters.seqAsJavaList(classification)) {
          appendNonNull(keys, "TAXON", checklistKey + "|" + n);
        }
      }

      return keys.stream().map(dictionary::get).filter(Objects::nonNull).toArray(Integer[]::new);
    }
  }

  /**
   * Generates map keys for the fields provided followed by a dictionary encoding based on the
   * provided dictionary. Any generated key not found in the dictionary will be dropped.
   */
  @AllArgsConstructor
  public static class NonTaxonMapKeysUDF
      implements UDF5<String, String, String, String, WrappedArray<String>, Integer[]>,
          Serializable {
    private Map<String, Integer> dictionary;

    @Override
    public Integer[] call(
        String datasetKey,
        String publishingOrgKey,
        String countryCode,
        String publishingCountry,
        WrappedArray<String> networkKeys) {

      Set<String> keys = new TreeSet<>(); // sorted, as we group by
      encodeToMapKey(
          keys, datasetKey, publishingOrgKey, countryCode, publishingCountry, networkKeys);
      return keys.stream().map(dictionary::get).filter(Objects::nonNull).toArray(Integer[]::new);
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

  /** Generate encoded map keys and add them to target if the provided values are not null. */
  private static void encodeToMapKey(
      Set<String> target,
      String datasetKey,
      String publishingOrgKey,
      String countryCode,
      String publishingCountry,
      WrappedArray<String> networkKeys) {
    appendNonNull(target, "ALL", 0);
    appendNonNull(target, "DATASET", datasetKey);
    appendNonNull(target, "PUBLISHER", publishingOrgKey);
    appendNonNull(target, "COUNTRY", countryCode);
    appendNonNull(target, "PUBLISHING_COUNTRY", publishingCountry);

    if (networkKeys != null && !networkKeys.isEmpty()) {
      for (String n : JavaConverters.seqAsJavaList(networkKeys)) {
        appendNonNull(target, "NETWORK", n);
      }
    }
  }

  /**
   * Generate the encoded map keys for taxa provided from the given checklist key. Keys are of the
   * structure 1:<checklistKey>|<taxonID>.
   */
  private static void encodeClassificationToMapKeys(
      Set<String> target, WrappedArray<String> classification, String checklistKey) {
    if (classification == null) return;

    for (String taxonId : JavaConverters.seqAsJavaList(classification)) {
      if (taxonId != null) {
        // encode the taxon key as "checklist|id"
        appendNonNull(target, "TAXON", checklistKey + "|" + taxonId);
      }
    }
  }

  /** Encodes the value into a map key provided it is not null */
  static void appendNonNull(Set<String> target, String prefix, Object value) {
    if (value != null) {
      Integer type = MAPS_TYPES.get(prefix);
      if (type != null) {
        target.add(type + ":" + value);
      }
    }
  }
}
