package org.gbif.maps.common.projection;

import java.awt.geom.AffineTransform;
import java.awt.geom.Point2D;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

/**
 * Test.
 */
class WinkelTripel {

  public static void main(String[] args) throws FactoryException {
    CoordinateReferenceSystem crs = CRS.parseWKT(
      new org.geotools.referencing.operation.projection.WinkelTripel.WinkelProvider().toWKT());
  }

}
