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
import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import no.ecc.vectortile.VectorTileEncoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.cache2k.extra.spring.SpringCache2kCacheManager;
import org.cache2k.io.CacheLoader;
import org.gbif.maps.CacheConfiguration;
import org.gbif.maps.common.hbase.ModulusSalt;
import org.gbif.maps.common.meta.MapMetastore;
import org.gbif.maps.common.meta.MapTables;
import org.gbif.maps.common.meta.Metastores;
import org.gbif.maps.io.PointFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
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
      .build();
  }

  /** Read from clickhouse and build the MVT */
  public Optional<byte[]> getTile(int z, long x, long y) {
    Map<String, Object> queryParams = new HashMap<>();
    queryParams.put("z", z);
    queryParams.put("x", x);
    queryParams.put("y", y);
    String sql = String.format(
      "          WITH\n" +
        "          bitShiftLeft(1::UInt64, {z:UInt8}) AS zoom_factor,\n" +
        "          bitShiftLeft(1::UInt64, 32 - {z:UInt8}) AS tile_size,\n" +
        "          tile_size * {x:UInt16} AS tile_x_begin,\n" +
        "          tile_size * ({x:UInt16} + 1) AS tile_x_end,\n" +
        "          tile_size * {y:UInt16} AS tile_y_begin,\n" +
        "          tile_size * ({y:UInt16} + 1) AS tile_y_end,\n" +
        "          mercator_x >= tile_x_begin AND mercator_x < tile_x_end\n" +
        "          AND mercator_y >= tile_y_begin AND mercator_y < tile_y_end AS in_tile,\n" +
        "          bitShiftRight(mercator_x - tile_x_begin, 32 - 10 - {z:UInt8}) AS x,\n" +
        "          bitShiftRight(mercator_y - tile_y_begin, 32 - 10 - {z:UInt8}) AS y,\n" +
        "          sum(occcount) AS occ_count\n" +
        "        SELECT x,y,occ_count\n" +
        "        FROM gbif_mercator\n" +  // EVERYTHING
        "        WHERE in_tile\n" +
        "        GROUP BY x,y");
    try (QueryResponse response = client.query(sql, queryParams, new QuerySettings()).get(3, TimeUnit.SECONDS);) {

      ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);

      if (!reader.hasNext()) {
        return Optional.empty();
      }
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
      LOG.error("Unexpected error loading from clickhouse.  Returning no tile.", e);
      return Optional.empty();
    }
  }
}
