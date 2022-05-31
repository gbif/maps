/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.maps.common.projection;

import com.google.common.annotations.VisibleForTesting;

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

  /**
   * For the given tile, returns the envelope for the tile, with a buffer.
   * @param z zoom
   * @param x tile X address
   * @param y tile Y address
   * @return an envelope for the tile, with the appropriate buffer
   */
  @VisibleForTesting
  @Override
  public Double2D[] tileBoundary(int z, long x, long y, double tileBuffer) {
    int tilesPerZoom = 1 << z;
    double degreesPerTile = 180d / tilesPerZoom;
    double bufferDegrees = tileBuffer * degreesPerTile;

    // the edges of the tile after buffering
    double minLng = to180Degrees((degreesPerTile * x) - 180 - bufferDegrees);
    double maxLng = to180Degrees(minLng + degreesPerTile + (bufferDegrees * 2));

    // clip the extent (ES barfs otherwise)
    double maxLat = Math.min(90 - (degreesPerTile * y) + bufferDegrees, 90);
    double minLat = Math.max(maxLat - degreesPerTile - 2 * bufferDegrees, -90);

    return new Double2D[]{new Double2D(minLng, minLat), new Double2D(maxLng, maxLat)};
  }

  /**
   * If the longitude is expressed from 0..360 it is converted to -180..180.
   */
  @VisibleForTesting
  static double to180Degrees(double longitude) {
    if (longitude > 180) {
      return longitude - 360;
    } else if (longitude < -180){
      return longitude + 360;
    }
    return longitude;
  }

  @Override
  public boolean isPolar() {
    return false;
  }

  @Override
  public boolean isPoleTile(int zoom, long x, long y) {
    return false;
  }
}
