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

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import no.ecc.vectortile.VectorTileEncoder;
import org.gbif.maps.common.filter.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * The data access service for clickhouse.
 */
public class ClickhouseMaps {
  private static final Logger LOG = LoggerFactory.getLogger(ClickhouseMaps.class);
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  private final Client client;

  public ClickhouseMaps() {
    client = new Client.Builder()
      .addEndpoint("http://clickhouse.gbif-dev.org:8123/")
      .setUsername("tim")
      .setPassword(null)
      .setSocketTimeout(10000) // expect filtered maps to fail otherwise
      .build();
  }

  // Decode the map keys into SQL. This may be refactoerd if we remove HBase and stop encoding mapKeys.
  private static final Map<String, String> REVERSE_MAP_TYPES = new ImmutableMap.Builder<String, String>()
    .put("0", "") // everything
    // Clickhouse does not support " AND %s IN(kingdomkey, phylumkey,.. ,taxonkey) " syntax
    .put("1", " AND (kingdomkey=%1$s OR phylumkey=%1$s OR classkey=%1$s OR orderkey=%1$s OR familykey=%1$s OR genuskey=%1$s OR specieskey=%1$s OR taxonkey=%1$s) ")
    .put("2", " AND datasetkey ='%s' ")
    .put("3", " AND publishingorgkey ='%s' ")
    .put("4", " AND country ='%s' ")
    .put("5", " AND publishingcountry ='%s' ")
    .put("6", " AND has(networkkey, '%s'::UUID) ")
    .build();

  private static final Map<String, String> TABLES = new ImmutableMap.Builder<String, String>()
    .put("EPSG:3857", "occurrence_mercator")
    .put("EPSG:4326", "occurrence_wgs84")
    .put("EPSG:3575", "occurrence_arctic")
    .put("EPSG:3031", "occurrence_antarctic")
    .build();

  public String getTileQuery(String mapKey, String epsg, Optional<Set<String>> basisOfRecords, Optional<Range> years, boolean verbose) {
    // SQL for filters
    String typeSQL = String.format(REVERSE_MAP_TYPES.get(mapKey.split(":")[0]), mapKey.split(":")[1]);
    String borSQL = basisOfRecords.isPresent() ?
      String.format(" AND basisofrecord IN ('%s') ", String.join("', '", basisOfRecords.get()))
      : "";
    String yearSQL = years.isPresent() && !years.get().isUnbounded() ? yearSQL(years.get()) : "";

    return String.format(
        "        WITH \n" +
        "          bitShiftLeft(1024::UInt64, 16 - {z:UInt8}) AS tile_size, \n" + // data was processed to zoom 16
        "          bitShiftRight(tile_size, 2) AS buffer, \n" + // 1/4 tile buffer
        "          tile_size * {x:UInt16} AS tile_x_begin, \n" +
        "          tile_size * ({x:UInt16} + 1) AS tile_x_end, \n" +
        "          tile_size * {y:UInt16} AS tile_y_begin, \n" +
        "          tile_size * ({y:UInt16} + 1) AS tile_y_end, \n" +
        "          x >= tile_x_begin - buffer AND x < tile_x_end + buffer \n" +
        "          AND y >= tile_y_begin - buffer AND y < tile_y_end + buffer AS in_tile, \n" +
        "          bitShiftRight(x - tile_x_begin, 16 - {z:UInt8}) AS local_x, \n" + // tile local coordinates
        "          bitShiftRight(y - tile_y_begin, 16 - {z:UInt8}) AS local_y, \n" +
        "          sum(occcount) AS occ_count\n" +
        "        SELECT local_x, local_y, occ_count \n" +
        "        FROM %s \n" +
        "        WHERE in_tile %s %s %s \n" +
        "        GROUP BY local_x, local_y",
      TABLES.get(epsg), typeSQL, borSQL, yearSQL);
  }

  private static String yearSQL(Range years) {
    Integer lower = years.getLower();
    Integer upper = years.getUpper();
    if (Objects.equals(lower, upper))  return (String.format(" AND year = %d", lower));
    else if (lower != null && upper != null) return (String.format(" AND year BETWEEN %d AND %d ", lower, upper));
    else if (lower != null) return (String.format(" AND year >= %d ", lower));
    else if (upper != null) return (String.format(" AND year < %d ", upper));
    else throw new IllegalArgumentException("Unexpected year range: " + years);
  }

  /** Read from clickhouse and build the MVT */
  public Optional<byte[]> getTile(int z, long x, long y, String mapKey, String epsg, Optional<Set<String>> basisOfRecords, Optional<Range> years, boolean verbose) {
    Map<String, Object> queryParams = new HashMap<>();
    queryParams.put("z", z);
    queryParams.put("x", x);
    queryParams.put("y", y);

    String sql = getTileQuery(mapKey, epsg, basisOfRecords, years, verbose);

    try (QueryResponse response = client.query(sql,
      queryParams, new QuerySettings()).get(10, TimeUnit.SECONDS);) {

      ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);

      if (!reader.hasNext())  return Optional.empty();

      VectorTileEncoder encoder = new VectorTileEncoder(1024, 32, false);
      while (reader.hasNext()) {
        reader.next();

        // get values
        long px = reader.getLong("local_x");
        long py = reader.getLong("local_y");
        long total = reader.getLong("occ_count");

        Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(px, py));
        Map<String, Object> meta = Maps.newHashMap();
        meta.put("total", total);
        encoder.addFeature("occurrence", meta, point);
      }
      return Optional.of(encoder.encode());
    } catch (Exception e) {
      e.printStackTrace();
      LOG.error("Unexpected error loading from clickhouse.  Returning no tile.", e);
      return Optional.empty();
    }
  }
}
