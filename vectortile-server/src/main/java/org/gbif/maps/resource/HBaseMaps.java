package org.gbif.maps.resource;

import org.gbif.maps.common.hbase.ModulusSalt;
import org.gbif.maps.common.meta.MapMetastore;
import org.gbif.maps.common.meta.MapTables;
import org.gbif.maps.common.meta.Metastores;
import org.gbif.maps.io.PointFeature;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The data access service for the map data which resides in HBase.
 * This implements a small cache since we recognise that map use often results in similar requests.
 */
public class HBaseMaps {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseMaps.class);
  private final Connection connection;
  private final MapMetastore metastore;
  private final ModulusSalt salt;

  public HBaseMaps(Configuration conf, String tableName, int saltModulus) throws Exception {
    connection = ConnectionFactory.createConnection(conf);

    if (tableName.contains("|")) {
      String[] tableNames = tableName.split("|");
      metastore = Metastores.newStaticMapsMeta(tableNames[0], tableNames[1]);
    }
    else {
      metastore = Metastores.newStaticMapsMeta(tableName, tableName); // backward compatible version
    }

    salt = new ModulusSalt(saltModulus);
  }

  public HBaseMaps(Configuration conf, MapMetastore metastore, int saltModulus) throws Exception {
    connection = ConnectionFactory.createConnection(conf);
    this.metastore = metastore;
    salt = new ModulusSalt(saltModulus);
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

  private LoadingCache<String, Optional<PointFeature.PointFeatures>> pointCache = CacheBuilder
    .newBuilder()
    .maximumSize(1000)
    .expireAfterAccess(1, TimeUnit.MINUTES)
    .build(
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
              return encoded != null ? Optional.of(PointFeature.PointFeatures.parseFrom(encoded)) : Optional.<PointFeature.PointFeatures>absent();
            } else {
              return Optional.absent();
            }
          }
        }
      }
    );

  private LoadingCache<TileKey, Optional<byte[]>> tileCache = CacheBuilder
    .newBuilder()
    .maximumSize(1000)
    .expireAfterWrite(1, TimeUnit.MINUTES)
    .build(
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
              return encoded != null ? Optional.of(encoded) : Optional.<byte[]>absent();
            } else {
              return Optional.absent();
            }
          }
        }
      }
    );

  /**
   * Returns a tile from HBase if one exists.
   */
  Optional<byte[]> getTile(String mapKey, String srs, int z, long x, long y) {
    try {
      return tileCache.get(new TileKey(mapKey, srs, z, x, y));
    } catch (ExecutionException e) {
      // there is nothing the caller can do.  Swallow this here, logging the error
      LOG.error("Unexpected error loading tile data from HBase.  Returning no tile.", e);
      return Optional.absent();
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
          return encoded != null ? Optional.of(encoded) : Optional.<byte[]>absent();
        } else {
          return Optional.absent();
        }
      }
    } catch (Exception e) {
      // there is nothing the caller can do.  Swallow this here, logging the error
      LOG.error("Unexpected error loading tile data from HBase.  Returning no tile.", e);
      return Optional.absent();
    }
  }

  Optional<String> getTileDate() {
    try {
      return Optional.of(metastore.read().getTileTableDate());
    } catch (Exception e) {
      // there is nothing the caller can do.  Swallow this here, logging the error
      LOG.error("Unexpected error loading tile data from HBase.  Returning no tile.", e);
      return Optional.absent();
    }
  }

  /**
   * Returns the point data from HBase if they exist.
   */
  Optional<PointFeature.PointFeatures> getPoints(String mapKey) {
    try {
      LOG.info("Getting points");
      return pointCache.get(mapKey);
    } catch (ExecutionException e) {
      // there is nothing the caller can do.  Swallow this here, logging the error
      LOG.error("Unexpected error loading point data from HBase.  Returning empty features collection.", e);
      return Optional.absent();
    }
  }

  Optional<String> getPointsDate() {
    try {
      return Optional.of(metastore.read().getPointTableDate());
    } catch (Exception e) {
      // there is nothing the caller can do.  Swallow this here, logging the error
      LOG.error("Unexpected error loading tile data from HBase.  Returning no tile.", e);
      return Optional.absent();
    }
  }

  /**
   * Container class to allow a single object for the caching key
   */
  private static class TileKey {
    final String rowKey;
    final String srs;
    final int z;
    final long x;
    final long y;

    TileKey(String rowKey, String srs, int z, long x, long y) {
      this.rowKey = rowKey;
      this.srs = srs;
      this.z = z;
      this.x = x;
      this.y = y;
    }

    String zxy() {
      return z + ":" + x + ":" + y;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      TileKey that = (TileKey) o;
      return Objects.equal(this.rowKey, that.rowKey) &&
             Objects.equal(this.srs, that.srs) &&
             Objects.equal(this.z, that.z) &&
             Objects.equal(this.x, that.x) &&
             Objects.equal(this.y, that.y);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(rowKey, srs, z, x, y);
    }
  }
}
