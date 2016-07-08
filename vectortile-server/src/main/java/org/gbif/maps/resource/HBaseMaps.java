package org.gbif.maps.resource;

import org.gbif.maps.io.PointFeature;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
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
  private final String tableName;

  public HBaseMaps(Configuration conf, String tableName) throws IOException {
    connection = ConnectionFactory.createConnection(conf);
    this.tableName = tableName;
  }

  private LoadingCache<String, Optional<PointFeature.PointFeatures>> pointCache = CacheBuilder
    .newBuilder()
    .maximumSize(1000)
    .expireAfterAccess(1, TimeUnit.MINUTES)
    .build(
      new CacheLoader<String, Optional<PointFeature.PointFeatures>>() {
        @Override
        public Optional<PointFeature.PointFeatures> load(String rowKey) throws Exception {
          try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Get get = new Get(Bytes.toBytes(rowKey));
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
          try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Get get = new Get(Bytes.toBytes(rowCell.rowKey));
            String columnFamily = rowCell.srs.replaceAll(":", "_").toUpperCase();
            get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(rowCell.zxy()));
            Result result = table.get(get);
            if (result != null) {
              byte[] encoded = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(rowCell.zxy()));
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
   * Returns the point data from HBase if they exist.
   */
  Optional<PointFeature.PointFeatures> getPoints(String mapKey) {
    try {
      return pointCache.get(mapKey);
    } catch (ExecutionException e) {
      // there is nothing the caller can do.  Swallow this here, logging the error
      LOG.error("Unexpected error loading point data from HBase.  Returning empty features collection.", e);
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
