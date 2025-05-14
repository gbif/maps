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
package org.gbif.maps.resource;

import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.util.SearchTypeValidator;
import org.gbif.api.util.VocabularyUtils;
import org.gbif.maps.common.filter.Range;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

/**
 * Utilities for dealing with the parameters and mappings.
 */
public class Params {
  // Patterns used in splitting strings
  static final Pattern COMMA = Pattern.compile(",");
  static final Pattern PIPE = Pattern.compile("[|]");

  // Parameters for dealing with hexagon binning
  static final String BIN_MODE_HEX = "hex";
  static final String BIN_MODE_SQUARE = "square";
  static final int HEX_TILE_SIZE = 4096;
  static final int SQUARE_TILE_SIZE = 4096;
  static final String DEFAULT_HEX_PER_TILE = "51";
  static final String DEFAULT_SQUARE_SIZE = "16";

  // Maps the HTTP parameter for the type to the HBase row key prefix for that map.
  // This aligns with the Spark processing that populates HBase of course, but maps the internal key to the
  // HTTP parameter.
  static final Map<String, String> MAP_TYPES = new ImmutableMap.Builder<String,String>()
    .put("taxonKey","1")
    .put("datasetKey","2")
    .put("publishingOrg", "3")
    .put("country", "4")
    .put("publishingCountry", "5")
    .put("networkKey", "6")
    .build();

  // The key for all maps
  static final String ALL_MAP_KEY = "0:0";

  /**
   * Open the tiles to the world (especially your friendly localhost developer!)
   * @param response
   */
  static void enableCORS(HttpServletResponse response) {
    response.addHeader("Allow-Control-Allow-Methods", "GET,HEAD,OPTIONS");
    response.addHeader("Access-Control-Allow-Origin", "*");
  }

  /**
   * Extracts the mapType:Key identifier from the request.
   * If an invalid request is provided (e.g. containing 2 types) then an IAE is thrown.
   * If no type is found, then the key for the all data map is given.
   *
   * @return The main map key, and an optional country mask key
   */
  static String[] mapKeys(HttpServletRequest request) {
    return mapKeys(request.getParameterMap());
  }

  /**
   * Extracts the mapType:Key identifier from the params.
   * @return The main map key, and an optional country mask key
   */
  public static String[] mapKeys(Map<String, String[]> params) {

    String mapKey = null,
    countryMaskKey = null;

    for (Map.Entry<String, String[]> param : params.entrySet()) {
      if (MAP_TYPES.containsKey(param.getKey())) {
        if (param.getValue().length!=1) {
          throw new IllegalArgumentException("Invalid request: Only one map may be requested. Perhaps you need to use ad-hoc mapping?");
        } else {
          if (param.getKey().equals("country")) {
            countryMaskKey = MAP_TYPES.get(param.getKey()) + ":" + param.getValue()[0];
          } else {
            if (mapKey != null) {
              throw new IllegalArgumentException("Invalid request: Only one type of map may be requested. Perhaps you need to use ad-hoc mapping?");
            }
            mapKey = MAP_TYPES.get(param.getKey()) + ":" + param.getValue()[0];
          }
        }
      }
    }

    // Rearrange if we only have a country
    if (mapKey == null && countryMaskKey != null) {
      mapKey = countryMaskKey;
      countryMaskKey = null;
    }

    if (mapKey == null && countryMaskKey == null) {
      mapKey = ALL_MAP_KEY;
    }

    return new String[]{mapKey, countryMaskKey};
  }

  /**
   * Converts the nullable encoded year into an array containing a minimum and maximum bounded range.
   * @param encodedYear Comma separated in min,max format (as per GBIF API)
   * @return An array of length 2, with the min and max year values, which may be NULL
   * @throws IllegalArgumentException if the year is unparsable
   */
  static Range toMinMaxYear(String encodedYear) {
    if (encodedYear == null) {
      return new Range(null,null);
    } else if (encodedYear.contains(",")) {
      String[] years = COMMA.split(encodedYear);
      if (years.length == 2) {
        Integer min = null;
        Integer max = null;
        if (!years[0].isEmpty()) {
          min = Integer.parseInt(years[0]);
        }
        if (!years[1].isEmpty()) {
          max = Integer.parseInt(years[1]);
        }
        return new Range(min, max);
      }
    } else {
      int year = Integer.parseInt(encodedYear);
      return new Range(year, year);
    }
    throw new IllegalArgumentException("Year must contain a single or a comma separated minimum and maximum value.  "
                                       + "Supplied: " + encodedYear);
  }

  /**
   * Iterates over the params map and adds to the search request the recognized parameters (i.e.: those that have a
   * correspondent value in the P generic parameter).
   * Empty (of all size) and null parameters are discarded.
   */
  static void setSearchParams(OccurrenceSearchRequest occurrenceSearchRequest, HttpServletRequest request) {
    for (Map.Entry<String, String[]> entry : request.getParameterMap().entrySet()) {
      OccurrenceSearchParameter p = findSearchParam(entry.getKey());
      if (p != null) {
        for (String val : removeEmptyParameters(entry.getValue())) {
          SearchTypeValidator.validate(p, val);
          occurrenceSearchRequest.addParameter(p, val);
        }
      }
    }
  }

  /**
   * @return The occurrence seach parameter for the HTTP Parameter name or null
   */
  private static OccurrenceSearchParameter findSearchParam(String name) {
    try {
      return OccurrenceSearchParameter.lookup(name).orElse(null);
    } catch (IllegalArgumentException e) {
      // we have all params here, not only the enum ones, so this is ok to end up here a few times
    }
    return null;
  }

  /**
   * Removes all empty and null parameters from the list.
   * Each value is trimmed(String.trim()) in order to remove all sizes of empty parameters.
   */
  private static List<String> removeEmptyParameters(String[] parameters) {
    List<String> cleanParameters = new ArrayList<>(parameters.length);
    for (String param : parameters) {
      final String cleanParam = Strings.nullToEmpty(param).trim();
      if (!cleanParam.isEmpty()) {
        cleanParameters.add(cleanParam);
      }
    }
    return cleanParameters;
  }
}
