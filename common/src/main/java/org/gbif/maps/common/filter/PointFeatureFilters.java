package org.gbif.maps.common.filter;

import org.gbif.maps.common.projection.Double2D;
import org.gbif.maps.common.projection.TileProjection;
import org.gbif.maps.common.projection.TileSchema;
import org.gbif.maps.common.projection.Tiles;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import no.ecc.vectortile.VectorTileEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.maps.io.PointFeature.PointFeatures.Feature;

/**
 * Filters and converters for PointFeature based tiles.
 */
public class PointFeatureFilters {
  private static final Logger LOG = LoggerFactory.getLogger(PointFeatureFilters.class);
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  /**
   * Encoders the features that fall within the buffered tile space when projected while also satisfying the given
   * year and basisOfRecord filters.
   *
   * @param encoder To collect into
   * @param layerName The layer within the VT to collect to
   * @param source To project and filter
   * @param projection To project the points
   * @param z The zoom level
   * @param x The target tile X address
   * @param y The target tile Y address
   * @param tileSize The tile size at which we are operating
   * @param buffer The buffer in pixels around the tile to support (typically same as that used in the encoder)
   * @param years The range of years that are to be included
   * @param basisOfRecords The basis of records that are to be included
   */
  public static void collectInVectorTile(VectorTileEncoder encoder, String layerName,
                                         List<Feature> source, TileProjection projection,
                                         TileSchema schema,
                                         int z, long x, long y, int tileSize, int buffer,
                                         Range years, Set<String> basisOfRecords) {

    // Note:  This projects the coordinates twice: once for filtering to the tile and secondly when collecting the
    //        tile features.  This could be optimized by calculating the WGS84 lat,lng of the tile+buffer extent and
    //        filtering the incoming stream using that.  At the time of writing the TileProjection does not offer that
    //        inverse capability and performance is in sub 5 ms, so not considered worthwhile (yet).
    AtomicInteger features = new AtomicInteger();
    source.stream()
          .filter(filterFeatureByBasisOfRecord(basisOfRecords))
          .filter(filterFeatureByYear(years))
          .filter(filterFeatureByTile(projection, schema, z, x, y, tileSize, buffer)) // filter to the tile
          .collect(
            // accumulate counts by year, for each pixel
            Collectors.groupingBy(toTileLocalPixelXY(projection, schema, z, x, y, tileSize, buffer),
                                  Collectors.groupingBy(Feature::getYear,Collectors.summingInt(Feature::getCount)))
          )
          .forEach((pixel, yearCounts) -> {
            // collect the year:count values as a feature within the VT for the pixel
            Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(pixel.getX(), pixel.getY()));
            Map<String, Object> meta = Maps.newHashMap();

            yearCounts.forEach((year, count) -> meta.put(String.valueOf(year), count));

            // add a total value across all years
            long sum = yearCounts.values().stream().mapToLong(v -> (Integer)v).sum();
            meta.put("total", sum);
            features.incrementAndGet();
            encoder.addFeature(layerName, meta, point);

            // Mercator is the only projection we support that has date line wrapping needs (4326 has 2 tiles in z0)
            if (schema == TileSchema.WEB_MERCATOR) {

              // Zoom 0 is a special case, whereby we copy data across the dateline into buffers
              if (z==0 && pixel.getX()<buffer) {
                Double2D adjustedPixel = new Double2D(pixel.getX() + tileSize, pixel.getY());
                Point point2 = GEOMETRY_FACTORY.createPoint(new Coordinate(adjustedPixel.getX(), adjustedPixel.getY()));
                encoder.addFeature(layerName, meta, point2);
              } else if (z==0 && pixel.getX()>=tileSize-buffer) {
                Double2D adjustedPixel = new Double2D(pixel.getX() - tileSize, pixel.getY());
                Point point2 = GEOMETRY_FACTORY.createPoint(new Coordinate(adjustedPixel.getX(), adjustedPixel.getY()));
                encoder.addFeature(layerName, meta, point2);
              }
            }
          });
    LOG.info("Collected {} features in the tile {}/{}/{}", features.get(), z, x, y);
  }

  /**
   * Provides a function that can project a feature into a pixel in tile local addressing space at that zoom.
   * @param projection To use in conversion
   * @param z Zoom level
   * @param x Tile x
   * @param y Tile y
   * @param tileSize That we operate with
   * @return The function to convert
   */
  public static Function<Feature, Double2D> toTileLocalPixelXY(final TileProjection projection, final TileSchema schema,
                                                               final int z, final long x, final long y,
                                                               final int tileSize, final int bufferSize) {
    return (f -> {
      Double2D pixelXY = projection.toGlobalPixelXY(f.getLatitude(), f.getLongitude(), z);
      return Tiles.toTileLocalXY(pixelXY, schema, z, x, y, tileSize, bufferSize);
    });
  }

  /**
   * Provides a predicate which can be used to filter Features that fall within the tile bounds when projected at the
   * given zoom level.
   * @param projection To use in conversion
   * @param z Zoom level
   * @param x Tile x
   * @param y Tile y
   * @param tileSize That we operate with
   * @param buffer The allowable buffer in pixels around the tile
   * @return
   */
  public static Predicate<Feature> filterFeatureByTile(final TileProjection projection, TileSchema schema,
                                                       final int z, final long x, final long y, final int tileSize,
                                                       final int buffer) {
    return (f -> {
      if (projection.isPlottable(f.getLatitude(), f.getLongitude())) {
        Double2D pixelXY = projection.toGlobalPixelXY(f.getLatitude(), f.getLongitude(), z);
        return Tiles.tileContains(z, x, y, tileSize, schema, pixelXY, buffer);
      } else {
        return false;
      }
    });
  }

  /**
   * Provides a predicate which can be used to filter Features for the accepted basisOfRecord
   * @param bors if provided gives the acceptable values
   * @return true if the conditions all pass, of false otherwise
   */
  public static Predicate<Feature> filterFeatureByBasisOfRecord(final Set<String> bors) {
    return (f -> {
      if (bors == null || bors.isEmpty()) {
        return true;
      } else {
        for (String bor : bors) {
          if (bor.equalsIgnoreCase(f.getBasisOfRecord().toString())) {
            return true;
          }
        }
        return false; // no basis of record match the given options
      }
    });
  }

  /**
   * Provides a predicate which can be used to filter Features for a year range.
   *
   * @param years The range of years which are to be filtered
   * @return true if the provided range is null or contains the feature year
   */
  public static Predicate<Feature> filterFeatureByYear(final Range years) {
    return (f -> years.isContained(f.getYear()));
  }
}
