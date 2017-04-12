package org.gbif.maps.common.projection;

/**
 * This simply plots the coordinates in world space performing no further projection.
 * Notes:
 * <ul>
 *   <li>This is an equirectangular projection with the Equator and Prime Meridian as the meridians, usually called Plate Careé.</li>
 *   <li>Tile schemes for projections other than Web Mercator aren't well-defined, but most tools default to two tiles
 *   at zoom 0, covering the whole world.</li>
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
    // Width and height of the area covered by the 0/0/0 tile (Western hemisphere) in pixels at this zoom.
    double pixels = (long) getTileSize() << zoom;
    double pixelsPerDegree = pixels / 180;
    double x = (longitude + 180) * pixelsPerDegree;
    double y = (-latitude + 90) * pixelsPerDegree; // inverted since top is pixel 0
    return new Double2D(x,y);
  }

  @Override
  public boolean isPlottable(double latitude, double longitude) {
    return latitude >= -90 && latitude <= 90 && longitude>=-180 && longitude<=180;
  }
}
