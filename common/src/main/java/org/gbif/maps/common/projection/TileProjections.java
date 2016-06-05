package org.gbif.maps.common.projection;

import java.awt.*;
import java.awt.geom.Point2D;

/**
 * Factories and utilities for dealing with coordinates and projecting those onto tiles.
 */
public class TileProjections {


  public static TileProjection mercator(int size) {
    return new PMercator(size);
  }

  public static TileProjection arctic(int size) {
    return new NorthPoleLAEAEurope(size);
  }

  public static TileProjection fromEPSG(String epsg, int size) {
    if ("EPSG:3857".equals(epsg)) {
      return mercator(size);
    } else if ("EPSG:3575".equals(epsg)) {
      return arctic(size);
    }
    throw new IllegalArgumentException("Unsupported EPSG supplied: " + epsg);
  }

  public static Point2D.Double toTileLocalXY(Point2D.Double globalPixelXY, long x, long y, int tileSize) {
    // it is tempting to us modulus math here, but that leads to incorrect results if you deal with tiles with buffers
    // since you start "seeing" points on the next tile
    return new Point2D.Double(globalPixelXY.getX() - x*tileSize, globalPixelXY.getY()- y*tileSize);
  }

  /**
   * Provides the tile address for the tile that will contain the pixel at the given zoom and tile size.
   * @param globalPixelXY The pixel address in global space
   * @param zoom the zoom level
   * @param tileSize the tile size
   * @return The tile XY address
   */
  public static LongPoint toTileXY(Point2D.Double globalPixelXY, int z, int tileSize) {
    long x = (long) Math.min(Math.max(globalPixelXY.getX() / tileSize, 0), Math.pow(2, z) - 1);
    long y = (long) Math.min(Math.max(globalPixelXY.getY() / tileSize, 0), Math.pow(2, z) - 1);
    return new LongPoint(x,y);
  }

  /**
   * Returns true if the global pixel falls on the tile located at x,y or within the given buffer.  A common buffer
   * might be 16 pixels when using 4096 tiles for example.
   * @param x The tile X address
   * @param y The tile Y address
   * @param globalPixelXY The pixel address to test
   * @param bufferPixels The buffer area to consider (negative values will not throw error but should be used with
   *                     extreme caution and are unexpected)
   * @return true if the the pixel falls on the tile or within the buffer zoo
   */
  public static boolean tileContains(long x, long y, int tileSize, Point2D.Double globalPixelXY, int bufferPixels) {
    return globalPixelXY.getX() >= x * tileSize - bufferPixels &&
           globalPixelXY.getX() <= x * tileSize + bufferPixels + tileSize &&
           globalPixelXY.getY() >= y * tileSize - bufferPixels &&
           globalPixelXY.getY() <= y * tileSize + bufferPixels + tileSize;
  }
}
