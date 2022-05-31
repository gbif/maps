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

import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.MathTransform;

/**
 * Our implementation of this Arctic projection indicates that it clips at the Equator through choice.
 * This class is threadsafe.
 * @see https://epsg.io/3575
 */
class NorthPoleLAEAEurope extends WGS84LambertAzimuthalEqualArea {
  static final String EPSG_CODE = "EPSG:3575";

  // A transform to convert from WGS84 coordinates into 3575 pixel space
  private static final MathTransform TRANSFORM;
  static {
    try {
      TRANSFORM = CRS.findMathTransform(CRS.decode("EPSG:4326"), CRS.decode("EPSG:3575"), true);
    } catch (FactoryException e) {
      throw new IllegalStateException("Unable to decode EPSG projections", e);
    }
  }

  @Override
  MathTransform getTransform() {
    return TRANSFORM;
  }

  NorthPoleLAEAEurope(int tileSize) {
    super(tileSize);
  }

  @Override
  public boolean isPlottable(double latitude, double longitude) {
    // clipped to Equator and above by deliberate choice, even though the projection would support more
    return latitude >= 0 && longitude>=-180 && longitude<=180;
  }

  @Override
  public boolean isPolar() {
    return true;
  }

  @Override
  public boolean isPoleTile(int zoom, long x, long y) {
    int tilesPerZoom = 1 << zoom;
    long quarter = tilesPerZoom/2;
    boolean vSeam = (x == quarter || x+1 == quarter);
    boolean hSeam = (y == quarter || y+1 == quarter);
    return zoom > 0 && (vSeam && hSeam);
  }
}
