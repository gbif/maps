package org.gbif.maps.common.projection;

import java.awt.*;
import java.io.Serializable;

/**
 * Provides a consistent inteface when dealing with projections and tiled views.
 * <p/>
 * Projecting is prone to developer error as it is common to deal with coordinates in different schemes in the same
 * code.  Some projections use X,Y addressing, others Y,X etc. and developers may be dealing with pixels addressed
 * globally or within a single tile within the space of a couple of lines of code.  This interface chooses careful
 * naming conventions and uses enums for strict enforcement of parameters to try and reduce errors.
 * <p/>
 * To enable use in e.g. Spark, all implementations must be serializable.
 */
public interface TileProjectionDEL extends Serializable {

  /**
   * The circumference of the earth at the equator in meters.
   */
  double EARTH_CIRCUMFERENCE = 40075016.686;

  /**
   * Explicit enumeration to declare whether e.g. pixels are addressed in GLOBAL space or on a TILE.
   * <p/>
   * When GLOBAL is used, the address is considered in world coordinates.  When TILE_LOCAL is used, the address is
   * considered related to the tile itself (e.g. between 0-255 for a 256 pixel tile).
   * <p/>
   * Note: This was added to aid readability and improve robustness.  The alternatives either proliferate methods or
   * require a booleans in method signatures which are prone to mistake.  We choose instead to force explicit
   * declaration, leading to more readable code.
   */
  enum AddressingScheme{GLOBAL, TILE_LOCAL};

  /**
   * Converts the coordinate to the pixel address, for the given zoom.
   * @param latitude To convert
   * @param longitude To convert
   * @param zoom The zoom level
   * @param scheme The addressing scheme to use in the pixel location
   * @return The pixel location addressed globally, or relative to the cornder of the tile in which it will fall
   * @throws IllegalArgumentException if the coordinate cannot be handled
   */
  Point.Double toPixelXY(double latitude, double longitude, int zoom, AddressingScheme scheme)
    throws IllegalArgumentException;

  /**
   * Return the address of the tile for the coordinate typical "Google" tile scheme addressing.
   * @param latitude To locate
   * @param zoom The zoom level
   * @return The tile address
   */
  LongPoint toTileXY(double latitude, double longitude, int zoom);

  /**
   * Indicates if the projection can plot the coordinate.
   * @param latitude Of the coordinate
   * @param longitude Of the coordinate
   * @return true if the projection can handle the coordinate
   */
  boolean isPlottable(double latitude, double longitude);
}
