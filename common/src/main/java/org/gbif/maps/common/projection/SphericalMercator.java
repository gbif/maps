package org.gbif.maps.common.projection;

/**
 * Spherical Mercator projection utilities which work at a given tile size.
 * This class is threadsafe.
 */
class SphericalMercator extends AbstractTileProjection {
  static final String EPSG_CODE = "EPSG:3857";

  SphericalMercator(int tileSize) {
    super(tileSize);
  }

  @Override
  public Double2D toGlobalPixelXY(double latitude, double longitude, int zoom) {
    double sinLatitude = Math.sin(latitude * (Math.PI / 180));
    double y = (0.5 - Math.log((1 + sinLatitude) / (1 - sinLatitude)) / (4 * Math.PI)) * ((long) getTileSize() << zoom);
    double x = (longitude + 180) / 360 * ((long) getTileSize() << zoom);
    return new Double2D(x,y);
  }

  @Override
  public boolean isPlottable(double latitude, double longitude) {
    return latitude >= -85.05 && latitude <= 85.05 && longitude>=-180 && longitude<=180;
  }
}
