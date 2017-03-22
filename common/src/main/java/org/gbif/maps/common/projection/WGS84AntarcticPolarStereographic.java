package org.gbif.maps.common.projection;

import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.MathTransform;

import java.awt.geom.AffineTransform;

/**
 * Antarctic projection.
 * See http://epsg.io/3031.
 */
class WGS84AntarcticPolarStereographic extends WGS84Azimuthal {
  static final String EPSG_CODE = "EPSG:3031";

  // A tranform to convert from WGS84 coordinates into 3031 pixel space
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
  static final double STEREOGRAPHIC_EXTENT = 12367396.21845986;

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
