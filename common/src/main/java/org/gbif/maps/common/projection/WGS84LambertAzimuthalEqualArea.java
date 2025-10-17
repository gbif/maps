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

import org.geotools.api.referencing.operation.MathTransform;


/**
 * Base class handling the transformations for a Lambert azimuthal equal-area projection.
 * Used in six projections, which are aligned to a different longitude:
 * <ul>
 *     <li>EPSG:3571 — WGS 84 / North Pole LAEA Bering Sea (180°E)</li>
 *     <li>EPSG:3572 — WGS 84 / North Pole LAEA Alaska (150°W)</li>
 *     <li>EPSG:3573 — WGS 84 / North Pole LAEA Canada (100°W)</li>
 *     <li>EPSG:3574 — WGS 84 / North Pole LAEA Atlantic (40°W)</li>
 *     <li>EPSG:3575 — WGS 84 / North Pole LAEA Europe (10°E)</li>
 *     <li>EPSG:3576 — WGS 84 / North Pole LAEA Russia (90°E)</li>
 * </ul>
 * See https://en.wikipedia.org/wiki/Lambert_azimuthal_equal-area_projection
 */
abstract class WGS84LambertAzimuthalEqualArea extends WGS84Azimuthal {
  // See http://gis.stackexchange.com/questions/149440/epsg3575-projected-bounds/149466#149466
  static final double LAEA_EXTENT = Math.sqrt(2.0) * AbstractTileProjection.EARTH_RADIUS;

  // An affine transform to move world coordinates into positive space addressing, so the lowest is 0,0
  static final AffineTransform OFFSET_TRANSFORM = AffineTransform.getTranslateInstance(LAEA_EXTENT, LAEA_EXTENT);

  @Override
  abstract MathTransform getTransform();

  WGS84LambertAzimuthalEqualArea(int tileSize) {
    super(tileSize);
  }

  @Override
  double getExtent() {
    return LAEA_EXTENT;
  }

  @Override
  public AffineTransform getOffsetTransform() {
    return OFFSET_TRANSFORM;
  }
}
