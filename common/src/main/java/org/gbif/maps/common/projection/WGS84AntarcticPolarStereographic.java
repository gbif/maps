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

import static org.gbif.maps.common.projection.WGS84.to180Degrees;

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

  /**
   * For the given tile, returns a WGS84 rectangular envelope for the tile, with a buffer.
   * @param z zoom
   * @param x tile X address
   * @param y tile Y address
   * @return an envelope for the tile, with the appropriate buffer
   */
  @Override
  public Double2D[] tileBoundary(int z, long x, long y, double tileBuffer) {
    if (z == 0) {
      return new Double2D[]{new Double2D(-180, -90), new Double2D(180, 0)};
    }

    System.out.println();
    int tilesPerZoom = 1 << z;

    double x1 = (x+0-tileBuffer)*getTileSize();
    double x2 = (x+1+tileBuffer)*getTileSize();
    double y1 = (y+0-tileBuffer)*getTileSize();
    double y2 = (y+1+tileBuffer)*getTileSize();

    long quarter = tilesPerZoom/2;

    boolean vSeam = (x == quarter || x+1 == quarter);
    boolean hSeam = (y == quarter || y+1 == quarter);
    boolean pole = vSeam && hSeam;

    System.out.println("Tile " + z + "/"+x+"/"+y + " buffer="+tileBuffer + (vSeam?" vSeam":"") + (hSeam?" hSeam":""));
    System.out.println("Width of extent px: " + tilesPerZoom + " * " + getTileSize() + " quarter="+quarter);
    System.out.println("Buffered tileBounds in px: (" + x1 + ", " + y1 + ") (" + x2 + ", " + y2 + ")");

    // Tile corners
    // (x1,y1) (x2,y1)
    // (x1,y2) (x2,y2)

    // Points used for longitudes should come from the two "circumfral" points.
    // Points used for latitudes should come from the two radial points.
    Double2D pForLat1, pForLat2, pForLng1, pForLng2;
    if ((x < quarter && y < quarter) || (x >= quarter && y >= quarter)) {
      // In the top-left or bottom-right quadrants:
      System.out.println("TL or BR");
      pForLat1 = fromGlobalPixelXY(x1, y1, z);
      pForLat2 = fromGlobalPixelXY(x2, y2, z);
      pForLng1 = fromGlobalPixelXY(x1, y2, z);
      pForLng2 = fromGlobalPixelXY(x2, y1, z);
      if (tileBuffer > 0) {
        // When a tile touches the 90° lines, calculate the buffer from the corner nearest the pole since this is larger.
        if (y+1 == quarter) { // bottom of top left quadrant
          pForLng1 = fromGlobalPixelXY(x2, y2, z);
        } else if (y == quarter) { // top of bottom right quadrant
          pForLng2 = fromGlobalPixelXY(x1, y1, z);
        }
        if (x+1 == quarter) { // right of top left quadrant
          pForLng2 = fromGlobalPixelXY(x2, y2, z);
        } else if (x == quarter) { // left of bottom right quadrant
          pForLng1 = fromGlobalPixelXY(x1, y1, z);
          // Over the antimeridian
          pForLng1 = new Double2D(pForLng1.getX(), pForLng1.getY()+360);
        }
      }
    } else {
      System.out.println("TR or BL");
      // In the top-right or bottom-left quadrants:
      pForLat1 = fromGlobalPixelXY(x2, y1, z);
      pForLat2 = fromGlobalPixelXY(x1, y2, z);
      pForLng1 = fromGlobalPixelXY(x1, y1, z);
      pForLng2 = fromGlobalPixelXY(x2, y2, z);
      if (tileBuffer > 0) {
        if (y+1 == quarter) { // bottom of top right quadrant
          pForLng2 = fromGlobalPixelXY(x1, y2, z);
        } else if (y == quarter) { // top of bottom left quadrant
          pForLng1 = fromGlobalPixelXY(x2, y1, z);
        }
        if (x+1 == quarter) { // right of bottom left quadrant
          pForLng2 = fromGlobalPixelXY(x2, y1, z);
          // Over the antimeridian
          pForLng2 = new Double2D(pForLng2.getX(), pForLng2.getY()-360);
        } else if (x + 1 == quarter) { // left of top right quadrant
          pForLng1 = fromGlobalPixelXY(x1, y2, z);
        }
      }
    }

    // If on the left half of the map, change any 180° longitudes to -180°
    if (x < quarter) {
      if (pForLng1.getY() == 180) pForLng1 = new Double2D(pForLng1.getX(), -180);
      if (pForLng2.getY() == 180) pForLng2 = new Double2D(pForLng2.getX(), -180);
    }

    System.out.println("pForLat1: " + pForLat1);
    System.out.println("pForLat2: " + pForLat2);
    System.out.println("pForLng1: " + pForLng1);
    System.out.println("pForLng2: " + pForLng2);

    double minLat = Math.min(pForLat1.getX(), pForLat2.getX());
    double maxLat = Math.min(Math.max(pForLat1.getX(), pForLat2.getX()), 0); // Maximum latitude is 0.
    double minLng = to180Degrees(Math.min(pForLng1.getY(), pForLng2.getY()));
    double maxLng = to180Degrees(Math.max(pForLng1.getY(), pForLng2.getY()));

    if (tileBuffer > 0 && pole) {
      minLat = -90;
      minLng = -180;
      maxLng =  180;
    }

    System.out.println("Lat range: " + minLat + "→" + maxLat);
    System.out.println("Lng range: " + (minLng) + "→" + (maxLng));

    return new Double2D[]{new Double2D(minLng, minLat), new Double2D(maxLng, maxLat)};
  }
}
