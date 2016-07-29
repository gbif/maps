package org.gbif.maps.common.filter;

import org.gbif.maps.common.projection.Double2D;
import org.gbif.maps.common.projection.TileProjection;
import org.gbif.maps.common.projection.Tiles;
import org.gbif.maps.io.PointFeature;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import no.ecc.vectortile.Filter;
import no.ecc.vectortile.VectorTileDecoder;
import no.ecc.vectortile.VectorTileEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filters and converters for dealing with VectorTiles.
 */
public class VectorTileFilters {
  private static final Logger LOG = LoggerFactory.getLogger(VectorTileFilters.class);
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
  private static final VectorTileDecoder DECODER = new VectorTileDecoder();

  // The key used in the metadata indicating a total count
  private static final String TOTAL_KEY = "total";

  static {
    DECODER.setAutoScale(false); // important to avoid auto scaling to 256 tiles
  }

  /**
   * Collects data (i.e. accumulates) which matches the filters given from the sourceTile into the encoder.
   * @param encoder To collect into
   * @param layerName The layer within the encoder into which we accumulate
   * @param sourceTile The source from which we are reading
   * @param z The zoom level
   * @param x The tile X coordinate of the target
   * @param y The tile Y coordinate of the target
   * @param sourceX The tile X coordinate of the source tile (it could be an adjacent tile)
   * @param sourceY The tile Y coordinate of the source tile (it could be an adjacent tile)
   * @param tileSize The tile size we are working with
   * @param bufferSize The buffer size to use for the source tile
   * @param years The year range filter to apply
   * @param basisOfRecords The basisOfRecords to flatter
   * @param verbose If true then individual years for each point will be included, otherwise only the totals
   *
   * @throws IOException
   */
  public static void collectInVectorTile(VectorTileEncoder encoder, String layerName, byte[] sourceTile,
                                         int z, long x, long y, long sourceX, long sourceY, int tileSize, int bufferSize,
                                         Range years, Set<String> basisOfRecords, boolean verbose)
    throws IOException {

    // We can push down the predicate for the basis of record filter into the decoder if it is supplied
    VectorTileDecoder.FeatureIterable features = basisOfRecords == null ? DECODER.decode(sourceTile) :
      DECODER.decode(sourceTile, basisOfRecords);

    // convert the features to a stream source filtering only to those on the tile and within the year range
    Iterable<VectorTileDecoder.Feature> iterable = () -> features.iterator();
    Stream<VectorTileDecoder.Feature> featureStream =
      StreamSupport.stream(iterable.spliterator(), false)
                   .filter(filterFeatureByTile(x,y,sourceX,sourceY,tileSize,bufferSize))
                   .filter(filterFeatureByYear(years));

    if (verbose) {
      // merge the data into yearCounts by pixels
      Map<Double2D, Map<String, Long>> pixels = featureStream.collect(
        // accumulate counts by year, for each pixel
        Collectors.toMap(
          toTileLocalPixelXY(x, y, sourceX, sourceY, tileSize),
          attributesPrunedToYears(years),
          (m1,m2) -> {
            m2.forEach((k, v) -> m1.merge(k, v, (v1, v2) -> {
              // accumulate because the same pixel can be present in different layers (basisOfRecords) in the
              // source tile
              Long.valueOf(v1).longValue();
              return ((Long)v1).longValue() + ((Long)v2).longValue();
            }));
            return m1;
          }
        ));

      // add the pixel to the encoder
      pixels.forEach((pixel, yearCounts) -> {

        Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(pixel.getX(), pixel.getY()));

        // add a total value across all years
        long sum = yearCounts.values().stream().mapToLong(v -> (Long)v).sum();
        yearCounts.put(TOTAL_KEY, sum);

        // If another feature exists at that pixel it is not our concern (there should not be)
        LOG.trace("Adding {} to {}", pixel, layerName);
        encoder.addFeature(layerName, yearCounts, point);

      });

    } else {
      // merge the data into total counts only by pixels
      Map<Double2D, Long> pixels = featureStream.collect(
        // accumulate totals for each pixel
        Collectors.toMap(
          toTileLocalPixelXY(x, y, sourceX, sourceY, tileSize),
          totalCountForYears(years), // note: we throw away year values here
          (m1,m2) -> {
            // simply accumulate totals
            return m1 + m2;
          }
        ));

      // add the pixel to the encoder
      pixels.forEach((pixel, total) -> {

        Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(pixel.getX(), pixel.getY()));

        Map<String, Object> meta = Maps.newHashMap();
        meta.put(TOTAL_KEY, total);

        // If another feature exists at that pixel it is not our concern (there should not be)
        LOG.trace("Adding {} to {}", pixel, layerName);
        encoder.addFeature(layerName, meta, point);

      });
    }
  }

  /**
   * Provides a function to accumulate the count within the year range (inclusive)
   */
  public static Function<VectorTileDecoder.Feature, Long> totalCountForYears(final Range years) {
    return new Function<VectorTileDecoder.Feature, Long>() {

      @Override
      public Long apply(VectorTileDecoder.Feature feature) {
        long runningCount = 0;

        for(Map.Entry<String, Object> e : feature.getAttributes().entrySet()) {
          try {
            Integer year = Integer.parseInt(e.getKey());
            if (years.isContained(year)) {
              runningCount += (Long) e.getValue();
            }
          } catch (NumberFormatException nfe) {
            LOG.warn("Unexpected non integer metadata entry {}:{}", e.getKey(), e.getValue());
          }
        }
        return runningCount;
      }
    };
  }

  /**
   * Provides a function to accumulate trim attributes to only include the year range desired.
   */
  public static Function<VectorTileDecoder.Feature, Map<String, Long>> attributesPrunedToYears(final Range years) {
    return new Function<VectorTileDecoder.Feature, Map<String, Long>>() {

      @Override
      public Map<String, Long> apply(VectorTileDecoder.Feature feature) {
        Map<String, Long> result = Maps.newHashMap();
        for(Map.Entry<String, Object> e : feature.getAttributes().entrySet()) {
          try {
            Integer year = Integer.parseInt(e.getKey());
            if (years.isContained(year)) {
              result.put(e.getKey(), (Long) e.getValue());
            }
          } catch (NumberFormatException nfe) {
            LOG.warn("Unexpected non integer metadata entry {}:{}", e.getKey(), e.getValue());
          }
        }
        return result;
      }
    };
  }

  /**
   * Gets the X,Y point for the feature.
   * @return The function to convert the feature to the location.
   */
  public static Function<VectorTileDecoder.Feature, Double2D> toTileLocalPixelXY(final long x, final long y,
                                                                                 final long sourceX, final long sourceY,
                                                                                 final int tileSize) {
    return new Function<VectorTileDecoder.Feature, Double2D>() {
      @Override
      public Double2D apply(VectorTileDecoder.Feature f) {
        if (f.getGeometry() instanceof Point) {
          Point p = (Point) f.getGeometry();

          // find the tile local pixel address on the target tile (pixel may be coming from an adjacent tile)
          Double2D pixelXY = new Double2D(tileSize * sourceX + p.getX(), tileSize * sourceY + p.getY());
          return Tiles.toTileLocalXY(pixelXY, x, y, tileSize);
        } else {
          throw new IllegalStateException("Only point geometries are supported");
        }
      }
    };
  }

  /**
   * Provides a predicate which can be used to filter Features for a year range.  If a min or max year bound is
   * given, then the feature must have a year present.
   *
   * @param minYear minimum year acceptable (inclusive) or null for unbounded
   * @param maxYear maximum year acceptable (inclusive) or null for unbounded
   * @return true if the conditions all pass, of false otherwise.
   */
  public static Predicate<VectorTileDecoder.Feature> filterFeatureByYear(final Range years) {
    return new Predicate<VectorTileDecoder.Feature>() {
      @Override
      public boolean test(VectorTileDecoder.Feature f) {

        // determine the extent of the year range within the tile
        int minFeatureYear = Integer.MAX_VALUE;
        int maxFeatureYear = Integer.MIN_VALUE;
        for (String yearAsStream : f.getAttributes().keySet()) {
          try {
            int y = Integer.parseInt(yearAsStream);
            minFeatureYear = minFeatureYear > y ? y : minFeatureYear;
            maxFeatureYear = maxFeatureYear < y ? y : maxFeatureYear;

            if (years.isContained(minFeatureYear) || years.isContained(maxFeatureYear)) {
              return true; // short circuit
            }

          } catch (Exception e) {
            // ignore attributes in unexpected formats
          }
        }

        // some of the years must overlap the filter range
        return years.isContained(minFeatureYear) || years.isContained(maxFeatureYear);
      }
    };
  }

  /**
   * Provides a predicate which can be used to filter features from the given tile that fall within the target tile
   * or within it's buffer.  This allows you to e.g. take data that has been prepared on tiles clipped to the hard
   * boundary of a tile and merge surrounding tiles into the target tile.
   *
   * @param x
   * @param y
   * @param sourceX
   * @param sourceY
   * @param tileSize
   * @param buffer
   * @return
   */
  public static Predicate<VectorTileDecoder.Feature> filterFeatureByTile(final long x, final long y,
                                                                         final long sourceX, final long sourceY,
                                                                         final int tileSize, final int bufferSize) {
    return new Predicate<VectorTileDecoder.Feature>() {
      @Override
      public boolean test(VectorTileDecoder.Feature f) {
        if (f.getGeometry() instanceof Point) {
          Point p = (Point) f.getGeometry();
          // global addressing of the pixel to consider filtering
          Double2D pixelXY = new Double2D(tileSize * sourceX + p.getX(), tileSize * sourceY + p.getY());
          return Tiles.tileContains(x, y, tileSize, pixelXY, bufferSize);
        } else {
          return false; // anything other than a point is unexpected, so we will simply skip gracefully
        }
      }
    };
  }

}
