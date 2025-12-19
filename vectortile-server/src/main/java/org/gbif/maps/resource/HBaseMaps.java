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

import org.cache2k.config.Cache2kConfig;

import org.gbif.maps.CacheConfiguration;
import org.gbif.maps.common.hbase.ModulusSalt;
import org.gbif.maps.common.meta.MapMetastore;
import org.gbif.maps.common.meta.MapTables;
import org.gbif.maps.common.meta.Metastores;
import org.gbif.maps.io.PointFeature;

import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.cache2k.Cache;
import org.cache2k.extra.spring.SpringCache2kCacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.google.protobuf.InvalidProtocolBufferException;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * The data access service for the map data which resides in HBase.
 * This implements a small cache since we recognise that map use often results in similar requests.
 */
public class HBaseMaps {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseMaps.class);
  private final Connection connection;
  private final MapMetastore metastore;
  private final ModulusSalt saltPoints;
  private final ModulusSalt saltTiles;
  private final Cache<String, Optional<PointFeature.PointFeatures>> pointCache;
  private final Cache<TileKey, Optional<byte[]>> tileCache;

  public HBaseMaps(Configuration conf, String tableName, int saltModulusPoints, int saltModulusTiles, SpringCache2kCacheManager cacheManager, MeterRegistry meterRegistry,
                   Cache2kConfig<String, Optional<PointFeature.PointFeatures>> pointCacheConfiguration,
                   Cache2kConfig<TileKey, Optional<byte[]>> tileCacheConfiguration) throws Exception {
    connection = ConnectionFactory.createConnection(conf);
    metastore = Metastores.newStaticMapsMeta(tableName, tableName); // backward compatible version
    saltPoints = new ModulusSalt(saltModulusPoints);
    saltTiles = new ModulusSalt(saltModulusTiles);
    pointCache = pointCacheBuilder(cacheManager, meterRegistry, pointCacheConfiguration);
    tileCache = tileCacheBuilder(cacheManager, meterRegistry, tileCacheConfiguration);
  }

  public HBaseMaps(Configuration conf, MapMetastore metastore, int saltModulusPoints, int saltModulusTiles, SpringCache2kCacheManager cacheManager, MeterRegistry meterRegistry,
                   Cache2kConfig<String, Optional<PointFeature.PointFeatures>> pointCacheConfiguration,
                   Cache2kConfig<TileKey, Optional<byte[]>> tileCacheConfiguration) throws Exception {
    connection = ConnectionFactory.createConnection(conf);
    this.metastore = metastore;
    saltPoints = new ModulusSalt(saltModulusPoints);
    saltTiles = new ModulusSalt(saltModulusTiles);
    pointCache = pointCacheBuilder(cacheManager, meterRegistry, pointCacheConfiguration);
    tileCache = tileCacheBuilder(cacheManager, meterRegistry, tileCacheConfiguration);
  }

  private TableName pointTable() throws Exception {
    MapTables meta = metastore.read();
    if (meta == null) {
      LOG.error("No point metadata exists");
      throw new IllegalStateException("Unable to read point metadata to locate table");
    }
    return TableName.valueOf(meta.getPointTable());
  }

  private TableName tileTable() throws Exception {
    MapTables meta = metastore.read();
    if (meta == null) {
      LOG.error("No tile metadata exists");
      throw new IllegalStateException("Unable to read tile metadata to locate table");
    }
    return TableName.valueOf(meta.getTileTable());
  }

  private Cache<String, Optional<PointFeature.PointFeatures>> pointCacheBuilder(SpringCache2kCacheManager manager, MeterRegistry meterRegistry, Cache2kConfig<String, Optional<PointFeature.PointFeatures>> pointCacheConfiguration) {
    manager.addCaches( b->
      pointCacheConfiguration.builder()
      .manager(manager.getNativeCacheManager())
      .name("pointCache")
      .loader(
        rowKey -> {
          LOG.info("Table {}", metastore.read().getPointTable());
          try (Table table = connection.getTable(pointTable())) {
            LOG.info(saltPoints.saltToString(rowKey));
            byte[] saltedKey = saltPoints.salt(rowKey);
            Get get = new Get(saltedKey);
            get.addColumn(Bytes.toBytes("EPSG_4326"), Bytes.toBytes("features"));
            Result result = table.get(get);
            if (result != null) {
              byte[] encoded = result.getValue(Bytes.toBytes("EPSG_4326"), Bytes.toBytes("features"));
              return Optional.ofNullable(encoded).map(e -> {
                try {
                  return PointFeature.PointFeatures.parseFrom(e);
                } catch (InvalidProtocolBufferException ex) {
                  throw new RuntimeException(ex);
                }
              });
            } else {
              return Optional.empty();
            }
          }
        }
      ));
    Cache<String, Optional<PointFeature.PointFeatures>> cache = manager.getNativeCacheManager().getCache("pointCache");
    CacheConfiguration.registerCacheMetrics(cache, meterRegistry);
    return cache;
  }

  private Cache<TileKey, Optional<byte[]>> tileCacheBuilder(SpringCache2kCacheManager manager, MeterRegistry meterRegistry, Cache2kConfig<TileKey, Optional<byte[]>> tileCacheConfiguration) {
    manager.addCaches( b ->
      tileCacheConfiguration.builder()
      .manager(manager.getNativeCacheManager())
      .name("tileCache")
      .loader(
        rowCell -> {
          try (Table table = connection.getTable(tileTable())) {
            String unsalted = rowCell.rowKey + ":" + rowCell.zxy();
            byte[] saltedKey = saltTiles.salt(unsalted);
            Get get = new Get(saltedKey);
            String columnFamily = rowCell.srs.replaceAll(":", "_").toUpperCase();
            get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("tile"));
            Result result = table.get(get);
            if (result != null) {
              byte[] encoded = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes("tile"));
              return Optional.ofNullable(encoded);
            } else {
              return Optional.empty();
            }
          }
        }
      ));

    Cache<TileKey, Optional<byte[]>> cache = manager.getNativeCacheManager().getCache("tileCache");
    CacheConfiguration.registerCacheMetrics(cache, meterRegistry);
    return cache;
  }

  /**
   * Returns a tile from HBase if one exists.
   */
  public Optional<byte[]> getTile(String mapKey, String srs, int z, long x, long y) {
    try {
      return tileCache.get(new TileKey(mapKey, srs, z, x, y));
    } catch (Exception e) {
      // there is nothing the caller can do.  Swallow this here, logging the error
      LOG.error("Unexpected error loading tile data from HBase.  Returning no tile.", e);
      return Optional.empty();
    }
  }

  /**
   * For testing.  Does not cache!
   * Returns a tile from HBase if one exists.
   */
  Optional<byte[]> getTileNoCache(String mapKey, String srs, int z, long x, long y) {
    Stopwatch timer = new Stopwatch().start();
    try {

      try (Table table = connection.getTable(tileTable())) {
        Get get = new Get(Bytes.toBytes(mapKey));
        String columnFamily = srs.replaceAll(":", "_").toUpperCase();
        get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(z + ":" + x + ":" + y));
        Result result = table.get(get);
        if (result != null) {
          byte[] encoded = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(z + ":" + x + ":" + y));
          LOG.info("HBase lookup of {} returned {}kb and took {}ms", z + ":" + x + ":" + y, encoded.length / 1024, timer.elapsedMillis());
          return Optional.of(encoded);
        } else {
          return Optional.empty();
        }
      }
    } catch (Exception e) {
      // there is nothing the caller can do.  Swallow this here, logging the error
      LOG.error("Unexpected error loading tile data from HBase.  Returning no tile.", e);
      return Optional.empty();
    }
  }

  Optional<String> getTileDate() {
    try {
      String date = metastore.read().getTileTableDate();
      return date != null ? Optional.of(date) : Optional.empty();
    } catch (Exception e) {
      // Unable to read from ZK or the metastore is not configured
      LOG.error("Unable to read the tile table date from ZK", e);
    }
    return Optional.empty();
  }

  /**
   * Returns the point data from HBase if they exist.
   */
  Optional<PointFeature.PointFeatures> getPoints(String mapKey) {
    try {
      LOG.info("Getting points");
      return pointCache.get(mapKey);
    } catch (Exception e) {
      // there is nothing the caller can do.  Swallow this here, logging the error
      LOG.error("Unexpected error loading point data from HBase.  Returning empty features collection.", e);
      return Optional.empty();
    }
  }

  Optional<String> getPointsDate() {
    try {
      String date = metastore.read().getPointTableDate();
      return date != null ? Optional.of(date) : Optional.empty();
    } catch (Exception e) {
      // Unable to read from ZK or the metastore is not configured
      LOG.error("Unable to read the point table date from ZK", e);
    }
    return Optional.empty();
  }

  /**
   * Container class to allow a single object for the caching key
   */
  @Data
  @AllArgsConstructor(access = AccessLevel.PACKAGE)
  public static class TileKey {
    final String rowKey;
    final String srs;
    final int z;
    final long x;
    final long y;

    String zxy() {
      return z + ":" + x + ":" + y;
    }

  }
}
