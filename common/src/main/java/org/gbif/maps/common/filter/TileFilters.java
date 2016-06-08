package org.gbif.maps.common.filter;

import org.gbif.maps.common.projection.Double2D;
import org.gbif.maps.common.projection.TileProjection;
import org.gbif.maps.common.projection.Tiles;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import no.ecc.vectortile.VectorTileEncoder;

import static org.gbif.maps.io.PointFeature.PointFeatures.Feature;

/**
 * Vector tile utilities.
 */
public class TileFilters {
  private static final int NULL_INT_VALUE = 0; // as specified in the protobuf file
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  /**
   * Collects the features in to the VT encoder that fall within the buffered tile space when projected and also
   * satisfy the given filters.
   *
   * @param encoder To collect into
   * @param layerName The layer within the VT to collect to
   * @param source To project and filter
   * @param projection To projectthe points
   * @param z The zoom level
   * @param x The target tile X address
   * @param y The target tile Y address
   * @param tileSize The tile size at which we are operating
   * @param buffer The buffer in pixels around the tile to support (typically same as that used in the encoder)
   * @param minYear The minimum year which must be satisfied if supplied
   * @param maxYear The maximum year which must be satisfied if supplied
   * @param basisOfRecords The BoRs that are supported
   */
  public static void collectInVectorTile(VectorTileEncoder encoder, String layerName,
                                         List<Feature> source, TileProjection projection,
                                         int z, long x, long y, int tileSize, int buffer,
                                         Integer minYear, Integer maxYear, String... basisOfRecords) {

    // Note:  This projects the coordinates twice: once for filtering to the tile and secondly when collecting the
    //        tile features.  This could be optimized by calculating the WGS84 lat,lng of the tile+buffer extent and
    //        filtering the incoming stream using that.  At the time of writing the TileProjection does not offer that
    //        inverse capability.
    source.stream()
          .filter(filterFeatureByBasisOfRecord(basisOfRecords))
          .filter(filterFeatureByYear(minYear, maxYear))
          .filter(filterFeatureByTile(projection, z, x, y, tileSize, buffer)) // filter to the tile
          .collect(
            // accumulate counts by year, for each pixel
            Collectors.groupingBy(toTileLocalPixelXY(projection, z, x, y, tileSize),
                                  Collectors.groupingBy(Feature::getYear,Collectors.summingInt(Feature::getCount)))
          )
          .forEach((pixel, yearCounts) -> {
            // collect the year:count values as a feature within the VT for the pixel
            Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(pixel.getX(), pixel.getY()));
            Map<String, Object> meta = Maps.newHashMap();
            yearCounts.forEach((year, count) -> meta.put(String.valueOf(year), count));
            encoder.addFeature(layerName, meta, point);
    });
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
  public static Function<Feature, Double2D> toTileLocalPixelXY(final TileProjection projection, final int z,
                                                               final long x, final long y, final int tileSize) {
    return new Function<Feature, Double2D>() {
      @Override
      public Double2D apply(Feature t) {
        Double2D pixelXY = projection.toGlobalPixelXY(t.getLatitude(),t.getLongitude(), z);
        return Tiles.toTileLocalXY(pixelXY, x, y, tileSize);
      }
    };
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
  public static Predicate<Feature> filterFeatureByTile(final TileProjection projection, final int z,
                                                       final long x, final long y, final int tileSize,
                                                       final int buffer) {
    return new Predicate<Feature>() {
      @Override
      public boolean test(Feature f) {
        if (projection.isPlottable(f.getLatitude(), f.getLongitude())) {
          Double2D pixelXY = projection.toGlobalPixelXY(f.getLatitude(), f.getLongitude(), z);
          return Tiles.tileContains(x, y, tileSize, pixelXY, buffer);
        } else {
          return false;
        }
      }
    };
  }

  /**
   * Provides a predicate which can be used to filter Features for the accepted basisOfRecord
   * @param bors if provided gives the acceptable values
   * @return true if the conditions all pass, of false otherwise
   */
  public static Predicate<Feature> filterFeatureByBasisOfRecord(final String... bors) {
    return new Predicate<Feature>() {
      @Override
      public boolean test(Feature f) {

        if (bors != null && bors.length > 0) {
          for (String bor : bors) {
            if (bor.equalsIgnoreCase(f.getBasisOfRecord().toString())) {
              return true;
            }
          }
          return false; // no basis of record match the given options
        } else {
          return true;
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
  public static Predicate<Feature> filterFeatureByYear(final Integer minYear, final Integer maxYear) {
    return new Predicate<Feature>() {
      @Override
      public boolean test(Feature f) {

        if (minYear != null && (f.getYear() == NULL_INT_VALUE || f.getYear() < minYear)) {
          return false;
        } else if (maxYear != null && (f.getYear() == NULL_INT_VALUE || f.getYear() > maxYear)) {
          return false;
        } else {
          return true;
        }
      }
    };
  }
}
