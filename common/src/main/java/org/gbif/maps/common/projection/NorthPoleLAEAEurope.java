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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.maps.common.projection.WGS84.to180Degrees;

/**
 * Our implementation of this Arctic projection indicates that it clips at the Equator through choice.
 * This class is threadsafe.
 * See https://epsg.io/3575
 */
class NorthPoleLAEAEurope extends WGS84LambertAzimuthalEqualArea {
  static final String EPSG_CODE = "EPSG:3575";

  private static final Logger LOG = LoggerFactory.getLogger(NorthPoleLAEAEurope.class);

  // A transform to convert from WGS84 coordinates into 3575 pixel space
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

  NorthPoleLAEAEurope(int tileSize) {
    super(tileSize);
  }

  @Override
  public boolean isPlottable(double latitude, double longitude) {
    // clipped to Equator and above by deliberate choice, even though the projection would support more
    return latitude >= 0 && longitude>=-180 && longitude<=180;
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
      return new Double2D[]{new Double2D(-180, 0), new Double2D(180, 90)};
    }

    int tilesPerZoom = 1 << z;

    // Unbuffered corners
    double x1u = (x+0)*getTileSize();
    double x2u = (x+1)*getTileSize();
    double y1u = (y+0)*getTileSize();
    double y2u = (y+1)*getTileSize();

    // Buffered corners
    double x1b = (x+0-tileBuffer)*getTileSize();
    double x2b = (x+1+tileBuffer)*getTileSize();
    double y1b = (y+0-tileBuffer)*getTileSize();
    double y2b = (y+1+tileBuffer)*getTileSize();

    // Tiles on the vertical and horizontal "seams" (180°, 90°, 0°, -90°) have this coordinate, or the next one.
    long seamCoordinate = tilesPerZoom/2;

    boolean pole = isPoleTile(z, x, y);

    if (LOG.isTraceEnabled()) {
      boolean vSeam = (x == seamCoordinate || x+1 == seamCoordinate);
      boolean hSeam = (y == seamCoordinate || y+1 == seamCoordinate);
      LOG.trace("");
      LOG.trace("Tile {}/{}/{} buffer={} {} {}", z, x, y, tileBuffer, vSeam ? "vSeam" : "", hSeam ? "hSeam" : "");
      LOG.trace("Width of extent px: {}×{}, seamCoordinate={}", tilesPerZoom, getTileSize(), seamCoordinate);
      LOG.trace("Unbuffered tileBounds in px: ({}, {}), ({}, {})", x1u, y1u, x2u, y2u);
      LOG.trace("Buffered tileBounds in px: ({}, {}), ({}, {})", x1b, y1b, x2b, y2b);
    }

    // Tile corners
    // A(x1,y1) B(x2,y1)
    // C(x1,y2) D(x2,y2)
    // The corners of the tile scheme are outside the projection.  Convert the NaNs returned to -Infinity instead.
    Double2D pA = nanToMinusInfinity(fromGlobalPixelXY(x1b, y1b, z));
    Double2D pB = nanToMinusInfinity(fromGlobalPixelXY(x2b, y1b, z));
    Double2D pC = nanToMinusInfinity(fromGlobalPixelXY(x1b, y2b, z));
    Double2D pD = nanToMinusInfinity(fromGlobalPixelXY(x2b, y2b, z));

    // Points used for longitudes should come from the two "circumfral" points.
    // Points used for latitudes should come from the two radial points.
    Double2D pForLat1, pForLat2;
    Double2D pForLng1, pForLng2;
    if ((x < seamCoordinate && y < seamCoordinate) || (x >= seamCoordinate && y >= seamCoordinate)) {
      // In the top-left or bottom-right quadrants:
      pForLat1 = pA;
      pForLat2 = pD;

      if (pole) {
        // Special case for poles: the ad-hoc request will make four requests across the world,
        // so no east-west buffering is required.
        pForLng1 = fromGlobalPixelXY(x1u, y2u, z); // C (unbuffered)
        pForLng2 = fromGlobalPixelXY(x2u, y1u, z); // B (unbuffered)
      } else {
        pForLng1 = pC;
        pForLng2 = pB;
      }

      if (tileBuffer > 0 && !pole) {
        // When a tile touches a 90° line (but not the pole),
        // calculate the buffer from the corner nearest the pole since this is larger.
        if (y+1 == seamCoordinate) { // bottom of top left quadrant
          pForLng1 = pD;
        } else if (y == seamCoordinate) { // top of bottom right quadrant
          pForLng2 = pA;
        }
        if (x+1 == seamCoordinate) { // right of top left quadrant
          Double2D pForLngAM = pD;
          // Over the antimeridian
          pForLng2 = new Double2D(pForLngAM.getX(), pForLngAM.getY()-360);
        } else if (x == seamCoordinate) { // left of bottom right quadrant
          pForLng1 = pA;
        }
      }
    } else {
      // In the top-right or bottom-left quadrants:
      pForLat1 = pB;
      pForLat2 = pC;

      if (pole) {
        pForLng1 = nanToMinusInfinity(fromGlobalPixelXY(x1u, y1u, z)); // A (unbuffered)
        pForLng2 = nanToMinusInfinity(fromGlobalPixelXY(x2u, y2u, z)); // D (unbuffered)
      } else {
        pForLng1 = pA;
        pForLng2 = pD;
      }

      if (tileBuffer > 0 && !pole) {
        if (y+1 == seamCoordinate) { // bottom of top right quadrant
          pForLng2 = pC;
        } else if (y == seamCoordinate) { // top of bottom left quadrant
          pForLng1 = pB;
        }
        if (x+1 == seamCoordinate) { // right of bottom left quadrant
          pForLng2 = pB;
        } else if (x == seamCoordinate) { // left of top right quadrant
          pForLng1 = pC;
        }
      }

      if (x == seamCoordinate) {
        // Over the antimeridian
        pForLng1 = new Double2D(pForLng1.getX(), pForLng1.getY()+360);
      }
    }

    // If on the left half of the map, change any 180° longitudes to -180°
    if (x < seamCoordinate) {
      if (pForLng1.getY() == 180) pForLng1 = new Double2D(pForLng1.getX(), -180);
      if (pForLng2.getY() == 180) pForLng2 = new Double2D(pForLng2.getX(), -180);
    }

    double minLat = Math.max(Math.min(pForLat1.getX(), pForLat2.getX()), 0);
    double maxLat = Math.max(Math.max(pForLat1.getX(), pForLat2.getX()), 0);
    double minLng = to180Degrees(Math.min(pForLng1.getY(), pForLng2.getY()));
    double maxLng = to180Degrees(Math.max(pForLng1.getY(), pForLng2.getY()));

    if (pole) {
      maxLat = 90;
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace("pForLat1: {}", pForLat1);
      LOG.trace("pForLat2: {}", pForLat2);
      LOG.trace("pForLng1: {}", pForLng1);
      LOG.trace("pForLng2: {}", pForLng2);
      LOG.trace("Lat range: {}→{}", minLat, maxLat);
      LOG.trace("Lng range: {}→{}", minLng, maxLng);
    }

    return new Double2D[]{new Double2D(minLng, minLat), new Double2D(maxLng, maxLat)};
  }

  private Double2D nanToMinusInfinity(Double2D p) {
    return new Double2D(Double.isNaN(p.getX()) ? Double.NEGATIVE_INFINITY : p.getX(), p.getY());
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
