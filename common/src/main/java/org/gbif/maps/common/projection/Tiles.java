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
 * Factories and utilities for dealing with coordinates and projecting those onto tiles.
 */
public class Tiles {

  /**
   * Factory of TileProjection for the given ESPG code.
   * @param epsg That defines the projection
   * @param size Of the tile in use
   * @return The TileProjection for the EPSG
   * @throws IllegalArgumentException If the EPSG is not supported
   */
  public static TileProjection fromEPSG(String epsg, int size) throws IllegalArgumentException {
    if (SphericalMercator.EPSG_CODE.equals(epsg)) {
      return new SphericalMercator(size);

    } else if (NorthPoleLAEAEurope.EPSG_CODE.equals(epsg)) {
      return new NorthPoleLAEAEurope(size);

    } else if (WGS84.EPSG_CODE.equalsIgnoreCase(epsg)) {
      return new WGS84(size);

    } else if (WGS84AntarcticPolarStereographic.EPSG_CODE.equalsIgnoreCase(epsg)) {
      return new WGS84AntarcticPolarStereographic(size);
    }
    throw new IllegalArgumentException("Unsupported EPSG supplied: " + epsg);
  }

  /**
   * Converts the pixel from global addressing to an address local to the tile identified.
   * @param globalPixelXY To convert
   * @param schema The tile schema to follow
   * @param z The zoom address of the tile we wish local addressing for
   * @param x The x address of the tile we wish local addressing for
   * @param y The y address of the tile we wish local addressing for
   * @param tileSize The tile size
   * @param bufferSize The buffer size
   * @return The pixel XY local to the tile in question
   */
  public static Long2D toTileLocalXY(Double2D globalPixelXY, TileSchema schema, int z, long x, long y, int tileSize, int bufferSize) {
    long numTilesAtZoom = schema.getZzTilesHorizontal() * 1<<z;
    long maxGlobalPixelAddress = numTilesAtZoom * tileSize;

    double localX = globalPixelXY.getX() - x * tileSize;
    double localY = globalPixelXY.getY() - y * tileSize;

    if (schema.isWrapX()) {
      if (schema.getZzTilesHorizontal() > 1 || z > 0) { // Don't wrap when the single tile is the global tile
        if (x == 0 && globalPixelXY.getX() >= maxGlobalPixelAddress - bufferSize) {
          localX = globalPixelXY.getX() - maxGlobalPixelAddress;
        } else if (x == numTilesAtZoom - 1 && globalPixelXY.getX() < bufferSize) {
          localX = globalPixelXY.getX() + maxGlobalPixelAddress - x * tileSize;
        }
      }
    }

    return new Long2D(Math.round(localX), Math.round(localY));
  }

  /**
   * Provides the tile address for the tile that will contain the pixel at the given zoom and tile size.
   *
   * @param globalPixelXY The pixel address in global space
   * @param z             the zoom level
   * @param tileSize      the tile size
   *
   * @return The tile XY address
   */
  public static Long2D toTileXY(Double2D globalPixelXY, TileSchema schema, int z, int tileSize) {
    long x = (long) Math.min(Math.max(globalPixelXY.getX() / tileSize, 0), Math.pow(2, z+schema.getZzTilesHorizontal()-1) - 1);
    long y = (long) Math.min(Math.max(globalPixelXY.getY() / tileSize, 0), Math.pow(2, z+schema.getZzTilesVertical()-1) - 1);
    return new Long2D(x, y);
  }

  /**
   * Returns true if the global pixel falls on the tile located at x,y or within the given buffer.
   * A common buffer might be 16 pixels when using 4096 tiles for example.
   *
   * @param z             The zoom level
   * @param x             The tile X address
   * @param y             The tile Y address
   * @param globalPixelXY The pixel address to test which should be at the corresponding zoom level of X,Y
   * @param bufferPixels  The buffer area to consider (negative values will not throw error but should be used with
   *                      extreme caution and are unexpected)
   *
   * @return true if the the pixel falls on the tile or within the buffer zone
   */
  public static boolean tileContains(int z, long x, long y, int tileSize, TileSchema schema, Double2D globalPixelXY,
                                     int bufferPixels) {

    boolean verticallyContained = globalPixelXY.getY() >= y * tileSize - bufferPixels &&
                                  globalPixelXY.getY() <= y * tileSize + bufferPixels + tileSize;


    boolean contained = globalPixelXY.getX() >= x * tileSize - bufferPixels &&
           globalPixelXY.getX() <= x * tileSize + bufferPixels + tileSize &&
           verticallyContained;

    // handle dateline wrapping if it is a match vertically
    if (!contained && verticallyContained) {
      long maxTileAddress = (long) Math.pow(2, z+schema.getZzTilesHorizontal()-1) - 1;

      // if the tile adjoins the dateline and it is a match vertically
      if (x == 0) {
        contained |= globalPixelXY.getX() >= tileSize - bufferPixels;
      } else if (x == maxTileAddress) {
        contained |= globalPixelXY.getX() < bufferPixels;
      }
    }
    return contained;
  }
}
