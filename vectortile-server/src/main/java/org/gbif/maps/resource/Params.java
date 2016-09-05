package org.gbif.maps.resource;

import org.gbif.maps.common.filter.Range;

import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.collect.ImmutableMap;

/**
 * Utilities for dealing with the parameters and mappings.
 */
class Params {
  // Patterns used in splitting strings
  static final Pattern COMMA = Pattern.compile(",");
  static final Pattern PIPE = Pattern.compile("[|]");

  // Maps the http parameter for the type to the HBase row key prefix for that map.
  // This aligns with the Spark processing that populates HBase of course, but maps the internal key to the
  // http parameter.
  static final Map<String, String> MAP_TYPES = ImmutableMap.of(
    "taxonKey","1",
    "datasetKey","2",
    "publishingOrganizationKey", "3",
    "country", "4",
    "publishingCountry", "5"
  );

  // The key for all maps
  static final String ALL_MAP_KEY = "0:0";

  /**
   * Open the tiles to the world (especially your friendly localhost developer!)
   * @param response
   */
  static void enableCORS(HttpServletResponse response) {
    response.addHeader("Allow-Control-Allow-Methods", "GET,OPTIONS");
    response.addHeader("Access-Control-Allow-Origin", "*");
  }

  /**
   * Extracts the mapType:Key identifier from the request.
   * If an invalid request is provided (e.g. containing 2 types) then an IAE is thrown.
   * If no type is found, then the key for the all data map is given.
   */
  static String mapKey(HttpServletRequest request) {
    Map<String, String[]> queryParams = request.getParameterMap();
    String mapKey = null;
    for (Map.Entry<String, String[]> param : queryParams.entrySet()) {
      if (MAP_TYPES.containsKey(param.getKey())) {
        if (mapKey != null || param.getValue().length!=1) {
          throw new IllegalArgumentException("Invalid request: Only one type of map may be requested.  "
                                             + "Hint: Perhaps you need to use ad hoc mapping?");
        } else {
          mapKey = MAP_TYPES.get(param.getKey()) + ":" + param.getValue()[0];
        }
      }
    }
    return mapKey == null ? ALL_MAP_KEY : mapKey;
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
        if (years[0].length() > 0) {
          min = Integer.parseInt(years[0]);
        }
        if (years[1].length() > 0) {
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
}
