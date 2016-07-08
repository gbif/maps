package org.gbif.maps.common.projection;

/**
 * This simply plots the coordinates in world space performing no further projection.
 * Notes:
 * <ul>
 *   <li>WGS84 might not be the best name for this.  It could be called a plate carreé projection for example but that
 *   is somewhat ambigious</li>
 *   <li>There is confusion as to what a slippy map should expect for Z0.  This implementation will result in a single
 *   square tile at zoom 0, with a buffer of empty space above and below the rectangular world</li>
 * </ul>
 *
 * This class is threadsafe.
 */
class WGS84 extends AbstractTileProjection {
  static final String EPSG_CODE = "EPSG:4326";

  WGS84(int tileSize) {
    super(tileSize);
  }

  @Override
  public Double2D toGlobalPixelXY(double latitude, double longitude, int zoom) {
    double pixels = (long) getTileSize() << zoom;
    double pixelsPerDegree = pixels / 360;
    double x = (longitude + 180) * pixelsPerDegree;
    double y = pixels - ((latitude * pixelsPerDegree) + pixels/2);  // inverted since top is pixel 0
    return new Double2D(x,y);
  }

  @Override
  public boolean isPlottable(double latitude, double longitude) {
    return latitude >= -90 && latitude <= 90 && longitude>=-180 && longitude<=180;
  }
}
