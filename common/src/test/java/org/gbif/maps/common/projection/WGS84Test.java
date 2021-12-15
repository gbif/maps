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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;

public class WGS84Test {

  @Test
  public void testToGlobalPixelXY() {
    WGS84 wgs84 = new WGS84(512);

    // Zoom 0 has two tiles, each 512×512, 2×1, giving a total area of 1024×512.
    assertEquals(new Double2D( 512,  256), wgs84.toGlobalPixelXY(  0,    0, 0));
    assertEquals(new Double2D(   0,    0), wgs84.toGlobalPixelXY( 90, -180, 0));
    assertEquals(new Double2D(1024,    0), wgs84.toGlobalPixelXY( 90,  180, 0));
    assertEquals(new Double2D(1024,  512), wgs84.toGlobalPixelXY(-90,  180, 0));
    assertEquals(new Double2D(   0,  512), wgs84.toGlobalPixelXY(-90, -180, 0));

    // Zoom 2 has 2×2²ⁿ = 2×2⁴ = 32 tiles, 8×4, giving a total area of 4096×2048.
    assertEquals(new Double2D(2048, 1024), wgs84.toGlobalPixelXY(  0,    0, 2));
    assertEquals(new Double2D(   0,    0), wgs84.toGlobalPixelXY( 90, -180, 2));
    assertEquals(new Double2D(4096,    0), wgs84.toGlobalPixelXY( 90,  180, 2));
    assertEquals(new Double2D(4096, 2048), wgs84.toGlobalPixelXY(-90,  180, 2));
    assertEquals(new Double2D(   0, 2048), wgs84.toGlobalPixelXY(-90, -180, 2));
  }

  @Test
  public void testTileBoundary() {
    WGS84 wgs84 = new WGS84(512);

    // Tile 0/0/0 and 0/1/0, no dateline
    // ■□
    Double2D[] expectedWest = new Double2D[]{new Double2D(-180, -90), new Double2D(  0, 90)};
    assertArrayEquals("0/0/0 without dateline failed", expectedWest, wgs84.tileBoundary(0, 0, 0, 0));

    // □■
    Double2D[] expectedEast = new Double2D[]{new Double2D(   0, -90), new Double2D( 180, 90)};
    assertArrayEquals("0/1/0 without dateline failed", expectedEast, wgs84.tileBoundary(0, 1, 0, 0));

    // zoom 1, with dateline

    // ■□□□
    // □□□□
    Double2D[] expectedNW = new Double2D[]{new Double2D(-180,   0), new Double2D(-90, 90)};
    assertArrayEquals("1/0/0 with dateline failed", expectedNW, wgs84.tileBoundary(1, 0, 0, 0));

    // □□□□
    // □□□■
    Double2D[] expectedSE = new Double2D[]{new Double2D(  90, -90), new Double2D(180,  0)};
    assertArrayEquals("1/3/1 with dateline failed", expectedSE, wgs84.tileBoundary(1, 3, 1, 0));

    double bufferInTiles = 0.25;

    Double2D[] expectedWestBuf = new Double2D[]{new Double2D(135, -90), new Double2D(45, 90)};
    assertArrayEquals("0/0/0 with dateline failed", expectedWestBuf, wgs84.tileBoundary(0, 0, 0, bufferInTiles));

    // □□□□
    // □□□■
    Double2D[] expectedSEBuf = new Double2D[]{new Double2D(67.5, -90), new Double2D(-157.5, 22.5)};
    assertArrayEquals("1/3/1 with dateline failed", expectedSEBuf, wgs84.tileBoundary(1, 3, 1, bufferInTiles));
  }
}
