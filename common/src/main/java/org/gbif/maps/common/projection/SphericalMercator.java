package org.gbif.maps.common.projection;

import static java.lang.Math.PI;
import static java.lang.Math.atan;
import static java.lang.Math.exp;
import static java.lang.Math.log;
import static java.lang.Math.sin;

/**
 * Spherical Mercator projection utilities which work at a given tile size.
 * This class is threadsafe.
 */
public class SphericalMercator extends AbstractTileProjection {
  static final String EPSG_CODE = "EPSG:3857";

  // The limit of the projection to get a square, about 85.05113°.
  public static final double MAX_LATITUDE = 180/PI * (2*atan(exp(PI)) - PI/2);

  SphericalMercator(int tileSize) {
    super(tileSize);
  }

  @Override
  public Double2D toGlobalPixelXY(double latitude, double longitude, int zoom) {
    double sinLatitude = sin(latitude * (PI / 180));
    double y = (0.5 - log((1 + sinLatitude) / (1 - sinLatitude)) / (4 * PI)) * ((long) getTileSize() << zoom);
    double x = (longitude + 180) / 360 * ((long) getTileSize() << zoom);
    return new Double2D(x,y);
  }

  @Override
  public boolean isPlottable(double latitude, double longitude) {
    return latitude >= -MAX_LATITUDE && latitude <= MAX_LATITUDE && longitude>=-180 && longitude<=180;
  }
}
