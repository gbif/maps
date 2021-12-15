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

import static java.lang.Math.PI;
import static java.lang.Math.atan;
import static java.lang.Math.exp;
import static java.lang.Math.log;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.pow;
import static java.lang.Math.sin;
import static java.lang.Math.sinh;
import static java.lang.Math.toDegrees;
import static org.gbif.maps.common.projection.WGS84.to180Degrees;

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

  @Override
  public Double2D[] tileBoundary(int z, long x, long y, double tileBuffer) {
    // At zoom zero, we just request the world.
    if (z == 0) {
      tileBuffer = 0;
    }

    double north = min(MAX_LATITUDE, max(-MAX_LATITUDE, tileLatitude(z, y-tileBuffer)));
    double south = min(MAX_LATITUDE, max(-MAX_LATITUDE, tileLatitude(z, y+tileBuffer + 1)));
    double  west = to180Degrees(tileLongitude(z,  x  - tileBuffer));
    double  east = to180Degrees(tileLongitude(z, x+1 + tileBuffer));

    return new Double2D[] {new Double2D(west, south), new Double2D(east, north)};
  }

  private static double tileLongitude(int z, double x) {
    return (x / pow(2.0, z) * 360.0 - 180);
  }

  private static double tileLatitude(int z, double y) {
    double n = PI - (2.0 * PI * y) / pow(2.0, z);
    return toDegrees(atan(sinh(n))) % 90;
  }
}
