package org.gbif.maps.common.projection;

import java.awt.*;
import java.awt.geom.Point2D;

public interface TileProjection {

  /**
   * Converts the coordinate to the global pixel address at the given zoom.
   * @param latitude To convert
   * @param longitude To convert
   * @param zoom The zoom level
   * @return The pixel location addressed globally
   */
  Point.Double toGlobalPixelXY(double latitude, double longitude, int zoom);

  boolean isPlottable(double latitude, double longitude);

  LongPoint toTileXY(Point2D.Double globalPixelXY, int zoom);

  Point2D.Double upperLeftPixel(long tileX, long tileY);

  Point2D.Double lowerRightPixel(long tileX, long tileY);
}
