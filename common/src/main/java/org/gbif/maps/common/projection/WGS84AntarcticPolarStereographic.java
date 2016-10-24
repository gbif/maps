package org.gbif.maps.common.projection;

import java.awt.geom.AffineTransform;
import java.awt.geom.Point2D;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.MathTransform;

/**
 * Antarctic projection.
 * See http://epsg.io/3031.
 */
class WGS84AntarcticPolarStereographic extends WGS84Stereographic {
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

  @Override
  MathTransform getTransform() {
    return TRANSFORM;
  }

  WGS84AntarcticPolarStereographic(int tileSize) {
    super(tileSize);
  }

  @Override
  public boolean isPlottable(double latitude, double longitude) {
    // clipped to equator and above by deliberate choice, even though the projection recommends 60 degrees south
    return latitude <= 0 && longitude>=-180 && longitude<=180;
  }
}

