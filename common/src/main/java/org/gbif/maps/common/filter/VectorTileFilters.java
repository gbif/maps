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
package org.gbif.maps.common.filter;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.maps.common.projection.Double2D;
import org.gbif.maps.common.projection.Long2D;
import org.gbif.maps.common.projection.TileSchema;
import org.gbif.maps.common.projection.Tiles;

import java.io.IOException;
import java.util.*;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import no.ecc.vectortile.VectorTileDecoder;
import no.ecc.vectortile.VectorTileEncoder;

/**
 * Optimized filters and converters for dealing with VectorTiles with reduced memory footprint.
 */
@Slf4j
public class VectorTileFilters {
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
  private static final VectorTileDecoder DECODER = new VectorTileDecoder();
  private static final String TOTAL_KEY = "total";

  static {
    DECODER.setAutoScale(false);
  }

  // Batch processing implementation for tile-collecting with coordinates
  public static void collectInVectorTile(VectorTileEncoder encoder, String layerName, byte[] sourceTile,
                                         TileSchema schema, int z, long x, long y, long sourceX, long sourceY,
                                         int tileSize, int bufferSize, Range years, Set<String> basisOfRecords,
                                         boolean verbose, int featuresBufferSize) throws IOException {

    VectorTileDecoder.FeatureIterable features = basisOfRecords == null
      ? DECODER.decode(sourceTile)
      : DECODER.decode(sourceTile, basisOfRecords);

    if (verbose) {
      processFeaturesVerboseWithCoords(encoder, layerName, features.iterator(), schema, z, x, y,
        sourceX, sourceY, tileSize, bufferSize, years, featuresBufferSize);
    } else {
      processFeaturesSimpleWithCoords(encoder, layerName, features.iterator(), schema, z, x, y,
        sourceX, sourceY, tileSize, bufferSize, years, featuresBufferSize);
    }
  }

  // Batch processing implementation for simple tile-collecting
  public static void collectInVectorTile(VectorTileEncoder encoder, String layerName, byte[] sourceTile,
                                         Range years, Set<String> basisOfRecords, boolean verbose, int featuresBufferSize)
    throws IOException {

    VectorTileDecoder.FeatureIterable features = basisOfRecords == null
      ? DECODER.decode(sourceTile)
      : DECODER.decode(sourceTile, basisOfRecords);

    if (verbose) {
      processFeaturesVerbose(encoder, layerName, features.iterator(), years, featuresBufferSize);
    } else {
      processFeaturesSimple(encoder, layerName, features.iterator(), years, featuresBufferSize);
    }

  }

  // Optimized mask implementation
  public static void maskTileByTile(VectorTileEncoder encoder, String layerName,
                                    byte[] sourceTile, byte[] maskTile) throws IOException {

    Set<Geometry> mask = new HashSet<>();
    for (VectorTileDecoder.Feature feature : DECODER.decode(maskTile)) {
      mask.add(feature.getGeometry());
    }
    log.debug("Mask contains {} geometries (points)", mask.size());

    for (VectorTileDecoder.Feature feature : DECODER.decode(sourceTile, layerName)) {
      if (mask.contains(feature.getGeometry())) {
        encoder.addFeature(layerName, feature.getAttributes(), feature.getGeometry());
      }
    }

    encoder.encode();
  }

  // Helper methods for batch processing with coordinates
  private static void processFeaturesVerboseWithCoords(VectorTileEncoder encoder, String layerName,
                                                       Iterator<VectorTileDecoder.Feature> iterator,
                                                       TileSchema schema, int z, long x, long y,
                                                       long sourceX, long sourceY, int tileSize,
                                                       int bufferSize, Range years, int featuresBufferSize) {
    Map<Long2D, Map<String, Long>> batch = new HashMap<>(featuresBufferSize);

    while (iterator.hasNext()) {
      VectorTileDecoder.Feature feature = iterator.next();

      if (!isFeatureInTile(feature, schema, z, x, y, sourceX, sourceY, tileSize, bufferSize) ||
        (!years.isUnbounded() && !isFeatureInYearRange(feature, years))) {
        continue;
      }

      Long2D pixel = toTileLocalPixelXY(feature, schema, z, x, y, sourceX, sourceY, tileSize, bufferSize);
      Map<String, Long> counts = extractYearCounts(feature, years);

      batch.merge(pixel, counts, (existing, newCounts) -> {
        newCounts.forEach((year, count) -> existing.merge(year, count, Long::sum));
        return existing;
      });

// Hotfix: Issue #99, this is broken as it causes duplicate points
//      if (batch.size() >= featuresBufferSize) {
//        flushBatchWithCoords(encoder, layerName, batch);
//        batch.clear();
//      }
    }

    if (!batch.isEmpty()) {
      flushBatchWithCoords(encoder, layerName, batch);
    }
  }

  private static void processFeaturesSimpleWithCoords(VectorTileEncoder encoder, String layerName,
                                                      Iterator<VectorTileDecoder.Feature> iterator,
                                                      TileSchema schema, int z, long x, long y,
                                                      long sourceX, long sourceY, int tileSize,
                                                      int bufferSize, Range years, int featuresBufferSize) {
    Map<Long2D, Long> batch = new HashMap<>(featuresBufferSize);

    while (iterator.hasNext()) {
      VectorTileDecoder.Feature feature = iterator.next();

      if (!isFeatureInTile(feature, schema, z, x, y, sourceX, sourceY, tileSize, bufferSize) ||
        (!years.isUnbounded() && !isFeatureInYearRange(feature, years))) {
        continue;
      }

      Long2D pixel = toTileLocalPixelXY(feature, schema, z, x, y, sourceX, sourceY, tileSize, bufferSize);
      long count = getTotalCount(feature, years);

      batch.merge(pixel, count, Long::sum);

// Hotfix: Issue #99, this is broken as it causes duplicate points
//      if (batch.size() >= featuresBufferSize) {
//        flushSimpleBatchWithCoords(encoder, layerName, batch);
//        batch.clear();
//      }
    }

    if (!batch.isEmpty()) {
      flushSimpleBatchWithCoords(encoder, layerName, batch);
    }
  }

  // Helper methods for batch processing without coordinates
  private static void processFeaturesVerbose(VectorTileEncoder encoder, String layerName,
                                             Iterator<VectorTileDecoder.Feature> iterator,
                                             Range years,
                                             int featuresBufferSize) {
    Map<Geometry, Map<String, Long>> batch = new HashMap<>(featuresBufferSize);

    while (iterator.hasNext()) {
      VectorTileDecoder.Feature feature = iterator.next();

      if (!years.isUnbounded() && !isFeatureInYearRange(feature, years)) {
        continue;
      }

      Geometry geom = feature.getGeometry();
      Map<String, Long> counts = extractYearCounts(feature, years);

      batch.merge(geom, counts, (existing, newCounts) -> {
        newCounts.forEach((year, count) -> existing.merge(year, count, Long::sum));
        return existing;
      });

// Hotfix: Issue #99, this is broken as it causes duplicate points
//      if (batch.size() >= featuresBufferSize) {
//        flushBatch(encoder, layerName, batch);
//        batch.clear();
//      }
    }

    if (!batch.isEmpty()) {
      flushBatch(encoder, layerName, batch);
    }
  }

  private static void processFeaturesSimple(VectorTileEncoder encoder, String layerName,
                                            Iterator<VectorTileDecoder.Feature> iterator,
                                            Range years,
                                            int featuresBufferSize) {
    Map<Geometry, Long> batch = new HashMap<>(featuresBufferSize);

    while (iterator.hasNext()) {
      VectorTileDecoder.Feature feature = iterator.next();

      if (!years.isUnbounded() && !isFeatureInYearRange(feature, years)) {
        continue;
      }

      Geometry geom = feature.getGeometry();
      long count = getTotalCount(feature, years);

      batch.merge(geom, count, Long::sum);

// Hotfix: Issue #99, this is broken as it causes duplicate points
//      if (batch.size() >= featuresBufferSize) {
//        flushSimpleBatch(encoder, layerName, batch);
//        batch.clear();
//      }
    }

    if (!batch.isEmpty()) {
      flushSimpleBatch(encoder, layerName, batch);
    }
  }

  // Flush methods for batch processing
  private static void flushBatchWithCoords(VectorTileEncoder encoder, String layerName,
                                           Map<Long2D, Map<String, Long>> batch) {
    batch.forEach((pixel, yearCounts) -> {
      long sum = yearCounts.values().stream().mapToLong(v -> v).sum();
      yearCounts.put(TOTAL_KEY, sum);
      Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(pixel.getX(), pixel.getY()));
      encoder.addFeature(layerName, yearCounts, point);
    });
  }

  private static void flushSimpleBatchWithCoords(VectorTileEncoder encoder, String layerName,
                                                 Map<Long2D, Long> batch) {
    batch.forEach((pixel, total) -> {
      Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(pixel.getX(), pixel.getY()));
      encoder.addFeature(layerName, Collections.singletonMap(TOTAL_KEY, total), point);
    });
  }

  private static void flushBatch(VectorTileEncoder encoder, String layerName,
                                 Map<Geometry, Map<String, Long>> batch) {
    batch.forEach((geom, yearCounts) -> {
      long sum = yearCounts.values().stream().mapToLong(v -> v).sum();
      yearCounts.put(TOTAL_KEY, sum);
      encoder.addFeature(layerName, yearCounts, geom);
    });
  }

  private static void flushSimpleBatch(VectorTileEncoder encoder, String layerName,
                                       Map<Geometry, Long> batch) {
    batch.forEach((geom, total) -> {
      encoder.addFeature(layerName, Collections.singletonMap(TOTAL_KEY, total), geom);
    });
  }

  // Feature processing utilities
  private static boolean isFeatureInYearRange(VectorTileDecoder.Feature feature, Range years) {
    if (years.isUnbounded())  {
      return true;
    }

    for (String key : feature.getAttributes().keySet()) {
      try {
        int year = Integer.parseInt(key);
        if (years.isContained(year)) {
          return true;
        }
      } catch (NumberFormatException e) {
        log.debug("Skipping non-year attribute key: {}", key);
      }
    }
    return false;
  }

  private static boolean isFeatureInTile(VectorTileDecoder.Feature feature, TileSchema schema,
                                         int z, long x, long y, long sourceX, long sourceY,
                                         int tileSize, int bufferSize) {
    if (!(feature.getGeometry() instanceof Point)) return false;

    Point p = (Point) feature.getGeometry();
    Double2D pixelXY = new Double2D(tileSize * sourceX + p.getX(), tileSize * sourceY + p.getY());
    return Tiles.tileContains(z, x, y, tileSize, schema, pixelXY, bufferSize);
  }

  private static Long2D toTileLocalPixelXY(VectorTileDecoder.Feature feature, TileSchema schema,
                                           int z, long x, long y, long sourceX, long sourceY,
                                           int tileSize, int bufferSize) {
    Point p = (Point) feature.getGeometry();
    Double2D pixelXY = new Double2D(tileSize * sourceX + p.getX(), tileSize * sourceY + p.getY());
    return Tiles.toTileLocalXY(pixelXY, schema, z, x, y, tileSize, bufferSize);
  }

  private static Map<String, Long> extractYearCounts(VectorTileDecoder.Feature feature, Range years) {
    Map<String, Long> counts = new HashMap<>();
    feature.getAttributes().forEach((key, value) -> {
      try {
        int year = Integer.parseInt(key);
        if (years.isUnbounded() || years.isContained(year)) {
          long count = value instanceof Number ? ((Number) value).longValue() : 1L;
          counts.put(key, count);
        }
      } catch (NumberFormatException e) {
        log.debug("Skipping non-year attribute key in yer count: {}", key);
      }
    });
    return counts;
  }

  private static long getTotalCount(VectorTileDecoder.Feature feature, Range years) {
    long total = 0L;
    for (Map.Entry<String, Object> entry : feature.getAttributes().entrySet()) {
      try {
        int year = Integer.parseInt(entry.getKey());
        if (years.isUnbounded() || years.isContained(year)) {
          total += entry.getValue() instanceof Number ? ((Number) entry.getValue()).longValue() : 1L;
        }
      } catch (NumberFormatException e) {
        log.debug("Skipping non-year attribute key in total count: {}", entry.getKey());
      }
    }
    return total;
  }
}
