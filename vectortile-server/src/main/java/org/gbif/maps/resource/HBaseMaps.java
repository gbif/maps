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

import no.ecc.vectortile.VectorTileEncoder;
import org.gbif.maps.CacheConfiguration;
import org.gbif.maps.common.filter.PointFeatureFilters;
import org.gbif.maps.common.filter.Range;
import org.gbif.maps.common.filter.VectorTileFilters;
import org.gbif.maps.common.hbase.ModulusSalt;
import org.gbif.maps.common.meta.MapMetastore;
import org.gbif.maps.common.meta.MapTables;
import org.gbif.maps.common.meta.Metastores;
import org.gbif.maps.common.projection.TileProjection;
import org.gbif.maps.common.projection.TileSchema;
import org.gbif.maps.common.projection.Tiles;
import org.gbif.maps.io.PointFeature;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.cache2k.extra.spring.SpringCache2kCacheManager;
import org.cache2k.io.CacheLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.google.protobuf.InvalidProtocolBufferException;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import static org.gbif.maps.resource.TileResource.LAYER_OCCURRENCE;

/**
 * The data access service for the map data which resides in HBase.
 * This implements a small cache since we recognise that map use often results in similar requests.
 */
public class HBaseMaps implements TileMaps {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseMaps.class);
  private final Connection connection;
  private final MapMetastore metastore;
  private final ModulusSalt salt;
  private final Cache<String, Optional<PointFeature.PointFeatures>> pointCache;
  private final Cache<TileKey, Optional<byte[]>> tileCache;

  public HBaseMaps(Configuration conf, String tableName, int saltModulus, SpringCache2kCacheManager cacheManager, MeterRegistry meterRegistry) throws Exception {
    connection = ConnectionFactory.createConnection(conf);
    metastore = Metastores.newStaticMapsMeta(tableName, tableName); // backward compatible version
    salt = new ModulusSalt(saltModulus);
    pointCache = pointCacheBuilder(cacheManager, meterRegistry);
    tileCache = tileCacheBuilder(cacheManager, meterRegistry);
  }

  public HBaseMaps(Configuration conf, MapMetastore metastore, int saltModulus, SpringCache2kCacheManager cacheManager, MeterRegistry meterRegistry) throws Exception {
    connection = ConnectionFactory.createConnection(conf);
    this.metastore = metastore;
    salt = new ModulusSalt(saltModulus);
    pointCache = pointCacheBuilder(cacheManager, meterRegistry);
    tileCache = tileCacheBuilder(cacheManager, meterRegistry);
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

  /**
   * Retrieves the data from HBase, applies the filters and merges the result into a vector tile containing a single
   * layer named {@link TileResource#LAYER_OCCURRENCE}.
   * <p>
   * This method handles both pre-tiled and simple feature list stored data, returning a consistent format of vector
   * tile regardless of the storage format.  Please note that the tile size can vary and should be inspected before use.
   *
   * @param z The zoom level
   * @param x The tile X address for the vector tile
   * @param y The tile Y address for the vector tile
   * @param mapKey The map key being requested
   * @param srs The SRS of the requested tile
   * @param basisOfRecords To include in the filter.  An empty or null value will include all values
   * @param years The year range to filter.
   * @return A byte array representing an encoded vector tile.  The tile may be empty.
   * @throws IOException Only if the data in HBase is corrupt and cannot be decoded.  This is fatal.
   */
  @Override
  public DatedVectorTile filteredVectorTile(
    int z, long x, long y,
    String mapKey, String srs,
    @Nullable Set<String> basisOfRecords, @NotNull Range years, boolean verbose, Integer tileSize, Integer bufferSize)
    throws IOException {

    Optional<byte[]> encoded = getTile(mapKey, srs, z, x, y);
    VectorTileEncoder encoder = new VectorTileEncoder(tileSize, bufferSize, false);

    String date;

    if (encoded.isPresent()) {
      date = getTileDate().orElse(null);
      LOG.info("Found tile {} {}/{}/{} for key {} with encoded length of {} and date {}", srs, z, x, y, mapKey, encoded.get().length, date);

      VectorTileFilters.collectInVectorTile(encoder, LAYER_OCCURRENCE, encoded.get(),
        years, basisOfRecords, verbose);
      return new DatedVectorTile(encoder.encode(), date);
    } else {
      // The tile size is chosen to match the size of prepared tiles.
      date = getPointsDate().orElse(null);
      Optional<PointFeature.PointFeatures> optionalFeatures = getPoints(mapKey);
      if (optionalFeatures.isPresent()) {
        TileProjection projection = Tiles.fromEPSG(srs, tileSize);
        PointFeature.PointFeatures features = optionalFeatures.get();
        LOG.info("Found {} features for key {}, date {}", features.getFeaturesCount(), mapKey, date);

        PointFeatureFilters.collectInVectorTile(encoder, LAYER_OCCURRENCE, features.getFeaturesList(),
          projection, TileSchema.fromSRS(srs), z, x, y, tileSize, bufferSize,
          years, basisOfRecords);
      }
      return new DatedVectorTile(encoder.encode(), date); // may be empty
    }
  }

  private Cache<String, Optional<PointFeature.PointFeatures>> pointCacheBuilder(SpringCache2kCacheManager manager, MeterRegistry meterRegistry) {

    manager.addCaches( b->
     new Cache2kBuilder<String, Optional<PointFeature.PointFeatures>>() {
    }
      .manager(manager.getNativeCacheManager())
      .name("pointCache")
      .entryCapacity(1_000)
      .expireAfterWrite(1, TimeUnit.MINUTES)
      .loader(
        new CacheLoader<String, Optional<PointFeature.PointFeatures>>() {
          @Override
          public Optional<PointFeature.PointFeatures> load(String rowKey) throws Exception {
            LOG.info("Table {}", metastore.read().getPointTable());
            try (Table table = connection.getTable(pointTable())) {
              LOG.info(salt.saltToString(rowKey));
              byte[] saltedKey = salt.salt(rowKey);
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
        }
      ));
    Cache<String, Optional<PointFeature.PointFeatures>> cache = manager.getNativeCacheManager().getCache("pointCache");
    CacheConfiguration.registerCacheMetrics(cache, meterRegistry);
    return cache;
  }

  private Cache<TileKey, Optional<byte[]>> tileCacheBuilder(SpringCache2kCacheManager manager, MeterRegistry meterRegistry) {
    manager.addCaches( b -> new Cache2kBuilder<TileKey, Optional<byte[]>>() {
    }
      .manager(manager.getNativeCacheManager())
      .name("tileCache")
      .entryCapacity(1_000)
      .expireAfterWrite(1, TimeUnit.MINUTES)
      .loader(
        new CacheLoader<TileKey, Optional<byte[]>>() {
          @Override
          public Optional<byte[]> load(TileKey rowCell) throws Exception {
            LOG.info("Table {}", metastore.read().getTileTable());
            try (Table table = connection.getTable(tileTable())) {
              String unsalted = rowCell.rowKey + ":" + rowCell.zxy();
              byte[] saltedKey = salt.salt(unsalted);
              LOG.info(salt.saltToString(unsalted));

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
  private static class TileKey {
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
