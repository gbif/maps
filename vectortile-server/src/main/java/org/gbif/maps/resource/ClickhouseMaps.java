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
import org.gbif.maps.TileServerConfiguration;
import org.gbif.maps.common.filter.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The data access service for clickhouse.
 */
public class ClickhouseMaps implements TileMaps {
  private static final Logger LOG = LoggerFactory.getLogger(ClickhouseMaps.class);
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  private final Client client;

  public ClickhouseMaps(TileServerConfiguration.ClickhouseConfiguration config) {
    client = new Client.Builder()
      .addEndpoint(config.getEndpoint())
      .setUsername(config.getUsername())
      .setPassword(config.getPassword())
      .enableConnectionPool(config.getEnableConnectionPool())
      .setConnectTimeout(config.getConnectTimeout())
      .setMaxConnections(config.getMaxConnections())
      .setMaxRetries(config.getMaxRetries())
      .setSocketTimeout(config.getSocketTimeout()) // expect filtered maps to fail otherwise
      .build();
  }

  // Decode the map keys into SQL. This may be refactored if we remove HBase and stop encoding mapKeys.
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

  /**
   * Retrieves the data from Clickhouse, applies the filters and merges the result into a vector tile containing a single
   * @param z The zoom level
   * @param x The tile X address for the vector tile
   * @param y The tile Y address for the vector tile
   * @param mapKey The map key being requested
   * @param srs The SRS of the requested tile
   * @param basisOfRecords To include in the filter.  An empty or null value will include all values
   * @param years The year range to filter.
   * @param verbose If true, the vector tile will contain record counts per year
   * @param tileSize The size of the tile in pixels
   * @param bufferSize The size of the buffer in pixels
   * @return DatedVectorTile containing the vector tile data
   */
  public DatedVectorTile filteredVectorTile(int z, long x, long y, String mapKey, String srs,
                                            @Nullable Set<String> basisOfRecords,
                                            @NotNull Range years, boolean verbose,
                                            Integer tileSize, Integer bufferSize
  ) {

    Optional<byte[]> ch = getTile(z, x, y, mapKey, srs, Optional.ofNullable(basisOfRecords), Optional.of(years), verbose);
    if (ch.isPresent()) {
      return new DatedVectorTile(ch.get(), null);
    } else {
      throw new RuntimeException("Clickhouse error for x=" + x + " y=" + y + " z=" + z + " mapKey=" + mapKey + " srs=" + srs + " basisOfRecords=" + basisOfRecords + " years=" + years + " verbose=" + verbose);
    }
  }

private String getTileQuery(String mapKey, String epsg, Optional<Set<String>> basisOfRecords, Optional<Range> years, boolean verbose, String where) {
    // SQL for filters
    String typeSQL = String.format(REVERSE_MAP_TYPES.get(mapKey.split(":")[0]), mapKey.split(":")[1]);
    String borSQL = basisOfRecords.map(bor -> String.format(" AND basisofrecord IN ('%s') ", String.join("', '", bor))).orElse("");
    String yearSQL = years.isPresent() && !years.get().isUnbounded() ? yearSQL(years.get()) : "";

    // verbose year handling into a map of "year:count", or otherwise a total sum
    String selectSQL = verbose ? "sumMap(map(year, occcount)) AS year_count" : "sum(occcount) AS occ_count";

    return String.format(
        "        WITH \n" +
        "          bitShiftLeft(1024::UInt64, 16 - {z:UInt8}) AS tile_size, \n" + // data was processed to zoom 16
        "          bitShiftRight(tile_size, 2) AS buffer, \n" + // 1/4 tile buffer
        "          tile_size * {x:UInt16} AS tile_x_begin, \n" + // tile boundaries
        "          tile_size * ({x:UInt16} + 1) AS tile_x_end, \n" +
        "          tile_size * {y:UInt16} AS tile_y_begin, \n" +
        "          tile_size * ({y:UInt16} + 1) AS tile_y_end, \n" +
        "          x >= tile_x_begin - buffer AND x < tile_x_end + buffer \n" +
        "          AND y >= tile_y_begin - buffer AND y < tile_y_end + buffer AS in_tile, \n" +
        "          bitShiftRight(x - tile_x_begin, 16 - {z:UInt8}) AS local_x, \n" + // tile local coordinates
        "          bitShiftRight(y - tile_y_begin, 16 - {z:UInt8}) AS local_y \n" +
        "        SELECT local_x, local_y, %s \n" +
        "        FROM %s \n" +
        "        WHERE in_tile %s %s %s %s\n" +  // optionally append "where", for dateline performance
        "        GROUP BY ALL",
      selectSQL, TABLES.get(epsg), typeSQL, borSQL, yearSQL, where);
  }

  private static String yearSQL(Range years) {
    Integer lower = years.getLower();
    Integer upper = years.getUpper();
    if (Objects.equals(lower, upper))  return (String.format(" AND year = %d", lower));
    else if (lower != null && upper != null) return (String.format(" AND year BETWEEN %d AND %d ", lower, upper));
    else if (lower != null) return (String.format(" AND year >= %d ", lower));
    else return (String.format(" AND year < %d ", upper));
  }

  /** Read from clickhouse and build the MVT */
  private Optional<byte[]> getTile(int z, long x, long y, String mapKey, String epsg, Optional<Set<String>> basisOfRecords, Optional<Range> years, boolean verbose) {
    Map<String, Object> queryParams = new HashMap<>();
    queryParams.put("z", z);
    queryParams.put("x", x);
    queryParams.put("y", y);

    VectorTileEncoder encoder = new VectorTileEncoder(1024, 64, false);

    boolean isGlobalMercator = "EPSG:3857".equals(epsg) && z==0; // special case for z0 dateline handling (one tile)

    String sql = getTileQuery(mapKey, epsg, basisOfRecords, years, verbose, "");
    queryAndAddToMVT(encoder, sql, queryParams, verbose, isGlobalMercator, 0);

    // For readability, dateline handling is achieved by requesting the adjacent tile and applying the adjustment  when
    // collecting the feature in the MVT. The alternative is unreadable SQL with the embedded adjustment and the
    // inability to use UInt and bit-shifting.
    if (("EPSG:3857".equals(epsg) && z>0) ||  ("EPSG:4326".equals(epsg))) {
      long maxTileX = "EPSG:3857".equals(epsg) ? (1l << z)-1 : (2l << z)-1; // 1 or 2 tiles per zoom
      if ( x ==  maxTileX) { // Eastern dateline
        queryParams.put("x", 0);
        sql = getTileQuery(mapKey, epsg, basisOfRecords, years, verbose, "AND x <= buffer");
        queryAndAddToMVT(encoder, sql, queryParams, verbose, false, 1024);
      }
      if ( x == 0 ) { // Western dateline
        queryParams.put("x", maxTileX);
        sql = getTileQuery(mapKey, epsg, basisOfRecords, years, verbose, "AND x >= tile_x_end-buffer");
        queryAndAddToMVT(encoder, sql, queryParams, verbose, false, -1024);
      }
    }

    return Optional.of(encoder.encode());
  }

  /**
   * Executes the SQL and adds the features into the MVT.
   */
  private void queryAndAddToMVT(VectorTileEncoder encoder, String sql, Map<String, Object> queryParams, boolean verbose, boolean isGlobalMercator, long xAdjustment) {
    try (QueryResponse response = client.query(sql,
      queryParams, new QuerySettings()).get(10, TimeUnit.SECONDS);
      ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response)
    ) {

      while (reader.hasNext()) {

        reader.next();

        // feature metadata with a total or the verbose year counts
        Map<String, Long> meta = Maps.newHashMap();
        if (!verbose) {
          long occCount = reader.getLong("occ_count");
          meta.put("total", occCount);

        } else {
          Map<Integer, BigInteger> yearCounts = reader.readValue("year_count");
          final AtomicLong total = new AtomicLong(0);
          yearCounts.forEach((k,v) -> {
            meta.put(String.valueOf(k), v.longValue());
            total.addAndGet(v.longValue());
          });
          meta.put("total", total.get());
        }

        long px = reader.getLong("local_x") + xAdjustment; // adjustment for date line handling
        long py = reader.getLong("local_y");
        Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(px, py));
        encoder.addFeature("occurrence", meta, point);

        // special case for Mercator Z0 buffer handling
        long buffer = 1024>>2; // match buffer calculation with SQL
        if (isGlobalMercator && px <= buffer) {
          point = GEOMETRY_FACTORY.createPoint(new Coordinate(px + 1024, py));
          encoder.addFeature("occurrence", meta, point);

        } else if (isGlobalMercator) {
          point = GEOMETRY_FACTORY.createPoint(new Coordinate(px - 1024, py));
          encoder.addFeature("occurrence", meta, point);
        }
      }
    } catch (Exception e) {
      LOG.error("Unexpected error loading from clickhouse.  Returning no tile.", e);
    }
  }
}
