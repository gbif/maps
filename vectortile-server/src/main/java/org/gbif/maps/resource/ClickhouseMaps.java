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

  public String getTileQuery(String mapKey, String srs, Optional<Set<String>> basisOfRecords, Optional<Range> years, boolean verbose) {
    // SQL for filters
    String typeSQL = String.format(REVERSE_MAP_TYPES.get(mapKey.split(":")[0]), mapKey.split(":")[1]);
    String borSQL = basisOfRecords.isPresent() ?
      String.format(" AND basisofrecord IN ('%s') ", String.join("', '", basisOfRecords.get()))
      : "";
    String yearSQL = years.isPresent() && !years.get().isUnbounded() ? yearSQL(years.get()) : "";

    return String.format(
        "        WITH \n" +
        "          bitShiftLeft(1::UInt64, {z:UInt8}) AS zoom_factor, \n" +
        "          bitShiftLeft(1::UInt64, 32 - {z:UInt8}) AS tile_size, \n" +
        "          bitShiftRight(tile_size, 4) AS buffer, \n" + // 1/16th tile buffer
        "          tile_size * {x:UInt16} AS tile_x_begin, \n" +
        "          tile_size * ({x:UInt16} + 1) AS tile_x_end, \n" +
        "          tile_size * {y:UInt16} AS tile_y_begin, \n" +
        "          tile_size * ({y:UInt16} + 1) AS tile_y_end, \n" +
        "          mercator_x >= tile_x_begin - buffer AND mercator_x < tile_x_end + buffer \n" +
        "          AND mercator_y >= tile_y_begin - buffer AND mercator_y < tile_y_end + buffer AS in_tile, \n" +
        "          bitShiftRight(mercator_x - tile_x_begin, 32 - 10 - {z:UInt8}) AS x, \n" +
        "          bitShiftRight(mercator_y - tile_y_begin, 32 - 10 - {z:UInt8}) AS y, \n" +
        "          sum(occcount) AS occ_count\n" +
        "        SELECT x,y,occ_count \n" +
        "        FROM gbif_mercator \n" +
        "        WHERE in_tile %s %s %s \n" +
        "        GROUP BY x,y",
      typeSQL, borSQL, yearSQL);
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
  public Optional<byte[]> getTile(int z, long x, long y, String mapKey, String srs, Optional<Set<String>> basisOfRecords, Optional<Range> years, boolean verbose) {
    Map<String, Object> queryParams = new HashMap<>();
    queryParams.put("z", z);
    queryParams.put("x", x);
    queryParams.put("y", y);

    String sql = getTileQuery(mapKey, srs, basisOfRecords, years, verbose);

    try (QueryResponse response = client.query(sql,
      queryParams, new QuerySettings()).get(10, TimeUnit.SECONDS);) {

      ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);

      if (!reader.hasNext())  return Optional.empty();

      VectorTileEncoder encoder = new VectorTileEncoder(1024, 32, false);
      while (reader.hasNext()) {
        reader.next();

        // get values
        long px = reader.getLong("x");
        long py = reader.getLong("y");
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
