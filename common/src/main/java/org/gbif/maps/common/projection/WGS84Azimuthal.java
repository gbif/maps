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
import java.awt.geom.Point2D;

import org.geotools.geometry.jts.JTS;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.opengis.referencing.operation.MathTransform;

/**
 * Base class handling azimuthal projections.
 */
abstract class WGS84Azimuthal extends AbstractTileProjection {
  static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  abstract MathTransform getTransform();

  // An affine transform to move world coordinates into positive space addressing, so the lowest is 0,0
  abstract AffineTransform getOffsetTransform();

  // The distance in projected units from the origin to the chosen limit of projected space.
  abstract double getExtent();

  WGS84Azimuthal(int tileSize) {
    super(tileSize);
  }

  @Override
  public Double2D toGlobalPixelXY(double latitude, double longitude, int zoom) {
    try {
      // NOTE: Axis order for EPSG:4326 is lat,lng so invert X=latitude, Y=longitude
      // Proven with if( CRS.getAxisOrder(sourceCRS) == CRS.AxisOrder.LAT_LON) {...}
      Point orig = GEOMETRY_FACTORY.createPoint(new Coordinate(latitude, longitude));
      Point p = (Point) JTS.transform(orig, getTransform()); // reproject

      // transform p which is in metres space into world pixel space
      Point2D.Double p2 = new Point2D.Double(p.getX(), p.getY());

      transformToWorldPixels(zoom).transform(p2,p2); // overwrites the source

      return new Double2D(p2.getX(), p2.getY());

    } catch (Exception e) {
      throw new IllegalStateException("Unable to reproject coordinates", e);
    }
  }

  public Double2D fromGlobalPixelXY(double globalX, double globalY, int zoom) {
    try {
      Point2D.Double gXY = new Point2D.Double(globalX, globalY);
      Point2D.Double aziXY = new Point2D.Double();
      transformToWorldPixels(zoom).inverseTransform(gXY, aziXY);

      Point azi = GEOMETRY_FACTORY.createPoint(new Coordinate(aziXY.x, aziXY.y));
      Point p = (Point) JTS.transform(azi, getTransform().inverse()); // reproject

      return new Double2D(p.getX(), p.getY());
    } catch (Exception e) {
      throw new IllegalStateException("Unable to reproject coordinates", e);
    }
  }

  /**
   * For the given zoom, provides a transformation which will be suitable to convert points into world pixel space.
   */
  AffineTransform transformToWorldPixels(int zoom) {
    // the world pixel range at this zoom
    double globalPixelExtent = zoom==0 ? getTileSize() : getTileSize() * (2<<(zoom-1));

    double pixelsPerMeter = globalPixelExtent / getExtent() / 2.0;
    AffineTransform scale = AffineTransform.getScaleInstance(pixelsPerMeter, pixelsPerMeter);

    // Swap Y to convert world addressing to pixel addressing where 0,0 is at the top
    AffineTransform mirror_y = new AffineTransform(1, 0, 0, -1, 0, globalPixelExtent);

    // combine the transform, noting you reverse the order
    AffineTransform world2pixel = new AffineTransform(mirror_y);
    world2pixel.concatenate(scale);
    world2pixel.concatenate(getOffsetTransform());
    return world2pixel;
  }

  @Override
  public Double2D[] tileBoundary(int zoom, long x, long y, double tileBuffer) {
    return new Double2D[0];
  }
}
