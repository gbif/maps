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

/**
 * Defines the interface for dealing with conversations between WGS84 referenced coordinates and pixels within tiles
 * at various zoom levels.
 */
public interface TileProjection {

  /**
   * Converts the coordinate to the global pixel address at the given zoom.
   * @param latitude To convert
   * @param longitude To convert
   * @param zoom The zoom level
   * @return The pixel location addressed globally
   */
  Double2D toGlobalPixelXY(double latitude, double longitude, int zoom);

  boolean isPlottable(double latitude, double longitude);

  /**
   * Returns a bounding box enveloping the given tile, with the given buffer.
   * @param zoom The zoom level
   * @param x The tile column
   * @param y The tile row
   * @param tileBuffer The buffer, in units of tiles.
   */
  Double2D[] tileBoundary(int zoom, long x, long y, double tileBuffer);

  /**
   * True if the projection is polar (includes the North or South Pole within its limit, rather than at the edge
   * or not at all).
   */
  boolean isPolar();

  /**
   * Returns true if the tile's corner touches a pole in a polar projection.
   */
  boolean isPoleTile(int zoom, long x, long y);
}
