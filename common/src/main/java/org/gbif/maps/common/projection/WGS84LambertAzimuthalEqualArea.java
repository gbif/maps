package org.gbif.maps.common.projection;

import java.awt.geom.AffineTransform;

import com.vividsolutions.jts.geom.GeometryFactory;
import org.opengis.referencing.operation.MathTransform;

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
 * @see https://en.wikipedia.org/wiki/Lambert_azimuthal_equal-area_projection
 */
abstract class WGS84LambertAzimuthalEqualArea extends WGS84Azimuthal {
  static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  // See http://gis.stackexchange.com/questions/149440/epsg3575-projected-bounds/149466#149466
  static final double LAEA_EXTENT = Math.sqrt(2.0) * AbstractTileProjection.EARTH_RADIUS;

  // An affine transform to move world coordinates into positive space addressing, so the lowest is 0,0
  static final AffineTransform OFFSET_TRANSFORM = AffineTransform.getTranslateInstance(LAEA_EXTENT, LAEA_EXTENT);

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
