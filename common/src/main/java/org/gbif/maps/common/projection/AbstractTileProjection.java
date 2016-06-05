package org.gbif.maps.common.projection;

import java.awt.*;
import java.awt.geom.Point2D;

public abstract class AbstractTileProjection implements TileProjection {

  /**
   * The circumference of the earth at the equator in meters.
   */
  //public static final double EARTH_CIRCUMFERENCE = 40075016.686; // seems to jump coords!!! TODO?
  public static final double EARTH_CIRCUMFERENCE = 40075160;



  private final int tileSize;

  public AbstractTileProjection(int tileSize) {
    if (tileSize == 256 || tileSize == 512 || tileSize == 1024 || tileSize == 2048 || tileSize == 4096) {
      this.tileSize = tileSize;
    } else {
      throw new IllegalArgumentException("Only the following tile sizes are supported: 256, 512, 1024, 2048, 4096.  "
                                         + "Supplied: " + tileSize);
    }
  }

  public int getTileSize(){
    return tileSize;
  }

  @Override
  public LongPoint toTileXY(Point2D.Double globalPixelXY, int zoom) {
    long x = (long) Math.min(Math.max(globalPixelXY.getX() / tileSize, 0), Math.pow(2, zoom) - 1);
    long y = (long) Math.min(Math.max(globalPixelXY.getY() / tileSize, 0), Math.pow(2, zoom) - 1);
    return new LongPoint(x,y);
  }

  @Override
  public Point2D.Double upperLeftPixel(long x, long y) {
    return new Point.Double(tileSize * x, tileSize * y);
  }

  @Override
  public Point2D.Double lowerRightPixel(long x, long y) {
    return new Point.Double((tileSize+1 * x)-1, (tileSize+1 * y)-1);
  }
}
