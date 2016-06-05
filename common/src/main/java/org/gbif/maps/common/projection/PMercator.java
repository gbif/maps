package org.gbif.maps.common.projection;

import java.awt.geom.Point2D;

/**
 * Spherical Mercator projection utilities which work at a given tile size.
 * This class is threadsafe.
 */
class PMercator extends AbstractTileProjection {

  public PMercator(int tileSize) {
    super(tileSize);
  }

  @Override
  public Point2D.Double toGlobalPixelXY(double latitude, double longitude, int zoom) {
    double sinLatitude = Math.sin(latitude * (Math.PI / 180));
    double y = (0.5 - Math.log((1 + sinLatitude) / (1 - sinLatitude)) / (4 * Math.PI)) * ((long) getTileSize() << zoom);
    double x = (longitude + 180) / 360 * ((long) getTileSize() << zoom);
    return new Point2D.Double(x,y);
  }

  @Override
  public boolean isPlottable(double latitude, double longitude) {
    return latitude >= -85.05 && latitude <= 85.05;
  }
}
