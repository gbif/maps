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

import java.awt.geom.AffineTransform;

import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.MathTransform;

/**
 * Antarctic projection.
 * See http://epsg.io/3031.
 */
class WGS84AntarcticPolarStereographic extends WGS84Azimuthal {
  static final String EPSG_CODE = "EPSG:3031";

  // A transform to convert from WGS84 coordinates into 3031 pixel space
  private static final MathTransform TRANSFORM;
  static {
    try {
      TRANSFORM = CRS.findMathTransform(CRS.decode("EPSG:4326"), CRS.decode(EPSG_CODE), true);
    } catch (FactoryException e) {
      throw new IllegalStateException("Unable to decode EPSG projections", e);
    }
  }

  /*
   * Calculated with x-coordinate of
   * (Point) JTS.transform(GEOMETRY_FACTORY.createPoint(new Coordinate(0, 0)), TRANSFORM)
   */
  static final double STEREOGRAPHIC_EXTENT = 12_367_396.21845986;

  // An affine transform to move world coordinates into positive space addressing, so the lowest is 0,0
  static final AffineTransform OFFSET_TRANSFORM = AffineTransform.getTranslateInstance(STEREOGRAPHIC_EXTENT, STEREOGRAPHIC_EXTENT);

  @Override
  MathTransform getTransform() {
    return TRANSFORM;
  }

  @Override
  double getExtent() {
    return STEREOGRAPHIC_EXTENT;
  }

  @Override
  public AffineTransform getOffsetTransform() {
    return OFFSET_TRANSFORM;
  }

  WGS84AntarcticPolarStereographic(int tileSize) {
    super(tileSize);
  }

  @Override
  public boolean isPlottable(double latitude, double longitude) {
    // clipped to equator and below by deliberate choice, even though the projection recommends 60° south
    return latitude <= 0 && longitude>=-180 && longitude<=180;
  }
}
