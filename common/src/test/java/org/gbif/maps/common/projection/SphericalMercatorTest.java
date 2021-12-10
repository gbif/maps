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

import org.junit.Test;

import static org.gbif.maps.common.projection.SphericalMercator.MAX_LATITUDE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.AssertOnDouble2D.assertEquals;

public class SphericalMercatorTest {

  static final double ε = 1e-5;

  static final double L85 = 85.0511287798066;

  @Test
  public void testIsPlottable() {
    SphericalMercator sm = new SphericalMercator(512);

    // The limit of the Spherical Mercator projection, a tiny bit over 85.05113°.
    assertTrue(sm.isPlottable(+L85, 0));
    assertTrue(sm.isPlottable(-L85, 0));

    // Just beyond that is off the map.
    assertFalse(sm.isPlottable(+L85+ε, 0));
    assertFalse(sm.isPlottable(-L85-ε, 0));
  }

  @Test
  public void testToGlobalPixelXY() {
    SphericalMercator sm = new SphericalMercator(512);

    // Zoom 0 has one tile, 512×512, 1×1, giving a total area of 512×512.
    assertEquals(new Double2D( 256,  256), sm.toGlobalPixelXY(   0,    0, 0), ε); // Middle of map
    assertEquals(new Double2D(   0,  256), sm.toGlobalPixelXY(   0, -180, 0), ε); // Left edge centre
    assertEquals(new Double2D( 512,  256), sm.toGlobalPixelXY(   0,  180, 0), ε); // Right edge centre
    assertEquals(new Double2D( 256,    0), sm.toGlobalPixelXY( L85,    0, 0), ε); // Top edge centre
    assertEquals(new Double2D( 256,  512), sm.toGlobalPixelXY(-L85,    0, 0), ε); // Bottom edge centre

    // Zoom 2 has 2ⁿ⁺¹ = 2³ = 16 tiles, 4×4, giving a total area of 2048×2048.
    assertEquals(new Double2D( 1024,  1024), sm.toGlobalPixelXY(   0,    0, 2), ε); // Middle of map
    assertEquals(new Double2D(    0,  1024), sm.toGlobalPixelXY(   0, -180, 2), ε); // Left edge centre
    assertEquals(new Double2D( 2048,  1024), sm.toGlobalPixelXY(   0,  180, 2), ε); // Right edge centre
    assertEquals(new Double2D( 1024,     0), sm.toGlobalPixelXY( L85,    0, 2), ε); // Top edge centre
    assertEquals(new Double2D( 1024,  2048), sm.toGlobalPixelXY(-L85,    0, 2), ε); // Bottom edge centre

    // Check these figures.
    assertEquals(new Double2D( 291.46666,148.19743), sm.toGlobalPixelXY(60.170833, 24.9375, 0), ε); // Helsinki
    assertEquals(new Double2D( 582.93333,296.39486), sm.toGlobalPixelXY(60.170833, 24.9375, 1), ε); // Helsinki
    assertEquals(new Double2D(1165.86666,592.78972), sm.toGlobalPixelXY(60.170833, 24.9375, 2), ε); // Helsinki
  }

  @Test
  public void testTileBoundary() {
    SphericalMercator sm = new SphericalMercator(512);

    // Tile 0/0/0
    // ■
    Double2D[] expected = new Double2D[]{new Double2D(-180, -MAX_LATITUDE), new Double2D(180, MAX_LATITUDE)};
    Double2D[] result = sm.tileBoundary(0, 0, 0, 0);
    assertEquals("0/0/0 failed", expected[0], result[0], ε);
    assertEquals("0/0/0 failed", expected[1], result[1], ε);

    // zoom 1

    // ■□
    // □□
    Double2D[] expectedNW = new Double2D[]{new Double2D(-180, 0), new Double2D(0, MAX_LATITUDE)};
    Double2D[] resultNW = sm.tileBoundary(1, 0, 0, 0);
    assertEquals("1/0/0 failed", expectedNW[0], resultNW[0], ε);
    assertEquals("1/0/0 failed", expectedNW[1], resultNW[1], ε);

    // □□
    // □■
    Double2D[] expectedSE = new Double2D[]{new Double2D(0, -MAX_LATITUDE), new Double2D(180, 0)};
    Double2D[] resultSE = sm.tileBoundary(1, 1, 1, 0);
    assertEquals("1/1/1 failed", expectedSE[0], resultSE[0], ε);
    assertEquals("1/1/1 failed", expectedSE[1], resultSE[1], ε);

    double bufferInTiles = 0.25;

    // Tile 0/0/0
    // ■
    Double2D[] expectedBuf = new Double2D[]{new Double2D(-180, -MAX_LATITUDE), new Double2D(180, MAX_LATITUDE)};
    Double2D[] resultBuf = sm.tileBoundary(0, 0, 0, bufferInTiles);
    assertEquals("0/0/0 with dateline failed", expectedBuf[0], resultBuf[0], ε);
    assertEquals("0/0/0 with dateline failed", expectedBuf[1], resultBuf[1], ε);

    // □□
    // □■
    Double2D[] expectedSEBuf = new Double2D[]{new Double2D( -45, -MAX_LATITUDE), new Double2D(-135,40.97990)};
    Double2D[] resultSEBuf = sm.tileBoundary(1, 1, 1, bufferInTiles);
    assertEquals("1/1/1 with dateline failed", expectedSEBuf[0], resultSEBuf[0], ε);
    assertEquals("1/1/1 with dateline failed", expectedSEBuf[1], resultSEBuf[1], ε);
  }
}
