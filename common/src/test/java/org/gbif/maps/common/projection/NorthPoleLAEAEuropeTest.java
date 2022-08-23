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

import org.geotools.referencing.operation.projection.MapProjection;
import org.junit.Test;

import static org.junit.AssertOnDouble2D.assertEquals;

public class NorthPoleLAEAEuropeTest {

  static double ε = 1e-5;

  @Test
  public void testToGlobalPixelXY() {
    NorthPoleLAEAEurope laeaE = new NorthPoleLAEAEurope(512);

    // LAEA Europe is orientated so 10°E is vertical.

    // Zoom 0 has one tile, 512×512, 1×1, giving a total area of 512×512.
    assertEquals(new Double2D( 256,  256), laeaE.toGlobalPixelXY(  90,    0, 0), ε); // Map centre
    assertEquals(new Double2D(   0,  256), laeaE.toGlobalPixelXY(   0,  -80, 0), ε); // Left edge centre
    assertEquals(new Double2D( 512,  256), laeaE.toGlobalPixelXY(   0,  100, 0), ε); // Right edge centre
    assertEquals(new Double2D( 256,    0), laeaE.toGlobalPixelXY(   0, -170, 0), ε); // Top edge centre
    assertEquals(new Double2D( 256,  512), laeaE.toGlobalPixelXY(   0,   10, 0), ε); // Bottom edge centre

    // Zoom 2 has 2ⁿ⁺¹ = 2³ = 16 tiles, 4×4, giving a total area of 2048×2048.
    assertEquals(new Double2D(1024, 1024), laeaE.toGlobalPixelXY(  90,    0, 2), ε); // Map centre
    assertEquals(new Double2D(   0, 1024), laeaE.toGlobalPixelXY(   0,  -80, 2), ε); // Left edge centre
    assertEquals(new Double2D(2048, 1024), laeaE.toGlobalPixelXY(   0,  100, 2), ε); // Right edge centre
    assertEquals(new Double2D(1024,    0), laeaE.toGlobalPixelXY(   0, -170, 2), ε); // Top edge centre
    assertEquals(new Double2D(1024, 2048), laeaE.toGlobalPixelXY(   0,   10, 2), ε); // Bottom edge centre
  }

  @Test
  public void testTileBoundary() {
    // This is only set here, somehow it must be true when the vectortile-service is running. Very odd.
    MapProjection.SKIP_SANITY_CHECKS = true;

    NorthPoleLAEAEurope leaeE = new NorthPoleLAEAEurope(512);

    {
      // Tile 0/0/0
      // ■
      Double2D[] expected = new Double2D[]{new Double2D(-180, 0), new Double2D(180, 90)};
      Double2D[] result = leaeE.tileBoundary(0, 0, 0, 0);
      assertEquals("0/0/0 failed", expected[0], result[0], ε);
      assertEquals("0/0/0 failed", expected[1], result[1], ε);
    }

    // zoom 1
    {
      // ■□
      // □□
      Double2D[] expected_1_0_0 = new Double2D[]{new Double2D(-170, 0), new Double2D(-80, 90)};
      Double2D[] result_1_0_0 = leaeE.tileBoundary(1, 0, 0, 0);
      assertEquals("1/0/0 failed", expected_1_0_0[0], result_1_0_0[0], ε);
      assertEquals("1/0/0 failed", expected_1_0_0[1], result_1_0_0[1], ε);
    }
    {
      // □■
      // □□
      Double2D[] expected_1_1_0 = new Double2D[]{new Double2D(100, 0), new Double2D(-170, 90)};
      Double2D[] result_1_1_0 = leaeE.tileBoundary(1, 1, 0, 0);
      assertEquals("1/1/0 failed", expected_1_1_0[0], result_1_1_0[0], ε);
      assertEquals("1/1/0 failed", expected_1_1_0[1], result_1_1_0[1], ε);
    }
    {
      // □□
      // ■□
      Double2D[] expected_1_0_1 = new Double2D[]{new Double2D(-80, 0), new Double2D(10, 90)};
      Double2D[] result_1_0_1 = leaeE.tileBoundary(1, 0, 1, 0);
      assertEquals("1/0/1 failed", expected_1_0_1[0], result_1_0_1[0], ε);
      assertEquals("1/0/1 failed", expected_1_0_1[1], result_1_0_1[1], ε);
    }
    {
      // □□
      // □■
      Double2D[] expected_1_1_1 = new Double2D[]{new Double2D(10, 0), new Double2D(100, 90)};
      Double2D[] result_1_1_1 = leaeE.tileBoundary(1, 1, 1, 0);
      assertEquals("1/1/1 failed", expected_1_1_1[0], result_1_1_1[0], ε);
      assertEquals("1/1/1 failed", expected_1_1_1[1], result_1_1_1[1], ε);
    }

    // zoom 2
    {
      // □□□□
      // □■□□
      // □□□□
      // □□□□
      Double2D[] expected_2_1_2 = new Double2D[]{new Double2D(-170, 30.111252), new Double2D(-80, 90)};
      Double2D[] result_2_1_1 = leaeE.tileBoundary(2, 1, 1, 0);
      assertEquals("2/1/1 failed", expected_2_1_2[0], result_2_1_1[0], ε);
      assertEquals("2/1/1 failed", expected_2_1_2[1], result_2_1_1[1], ε);
    }
    {
      // □□□□
      // □□□■
      // □□□□
      // □□□□
      Double2D[] expected_2_3_1 = new Double2D[]{new Double2D(100, 0), new Double2D(145, 48.717627)};
      Double2D[] result_2_3_1 = leaeE.tileBoundary(2, 3, 1, 0);
      assertEquals("2/3/1 failed", expected_2_3_1[0], result_2_3_1[0], ε);
      assertEquals("2/3/1 failed", expected_2_3_1[1], result_2_3_1[1], ε);
    }
    {
      // □□□□
      // □□□□
      // □□□□
      // □■□□
      Double2D[] expected_2_1_3 = new Double2D[]{new Double2D(-35, 0), new Double2D(10, 48.717627)};
      Double2D[] result_2_1_3 = leaeE.tileBoundary(2, 1, 3, 0);
      assertEquals("2/1/3 failed", expected_2_1_3[0], result_2_1_3[0], ε);
      assertEquals("2/1/3 failed", expected_2_1_3[1], result_2_1_3[1], ε);
    }
    {
      // □□□□
      // □□□□
      // □□□□
      // □□□■
      Double2D[] expected_2_3_3 = new Double2D[]{new Double2D(36.565051, 0), new Double2D(73.434949, 30.111252)};
      Double2D[] result_2_3_3 = leaeE.tileBoundary(2, 3, 3, 0);
      assertEquals("2/3/3 failed", expected_2_3_3[0], result_2_3_3[0], ε);
      assertEquals("2/3/3 failed", expected_2_3_3[1], result_2_3_3[1], ε);
    }

    // zoom 3
    {
      // □□□□  □□□□
      // □□■□  □□□□
      // □□□□  □□□□
      // □□□□  □□□□
      //
      // □□□□  □□□□
      // □□□□  □□□□
      // □□□□  □□□□
      // □□□□  □□□□
      Double2D[] expected3_5_3 = new Double2D[]{new Double2D(100, 43.560659), new Double2D(145, 69.719412)};
      Double2D[] result3_5_3 = leaeE.tileBoundary(3, 5, 3, 0);
      assertEquals("3/5/3 failed", expected3_5_3[0], result3_5_3[0], ε);
      assertEquals("3/5/3 failed", expected3_5_3[1], result3_5_3[1], ε);
    }

    double bufferInTiles = 0.25;

    {
      // Tile 0/0/0
      // ■
      Double2D[] expectedBuf = new Double2D[]{new Double2D(-180, 0), new Double2D(180, 90)};
      Double2D[] resultBuf = leaeE.tileBoundary(0, 0, 0, bufferInTiles);
      assertEquals("0/0/0 with buffer failed", expectedBuf[0], resultBuf[0], ε);
      assertEquals("0/0/0 with buffer failed", expectedBuf[1], resultBuf[1], ε);
    }

    {
      // □□
      // □■
      Double2D[] expected_1_1_1 = new Double2D[]{new Double2D(10, 0), new Double2D(100, 90)};
      leaeE.tileBoundary(1, 1, 1, 0);
      Double2D[] result_1_1_1 = leaeE.tileBoundary(1, 1, 1, bufferInTiles);
      assertEquals("1/1/1 with buffer failed", expected_1_1_1[0], result_1_1_1[0], ε);
      assertEquals("1/1/1 with buffer failed", expected_1_1_1[1], result_1_1_1[1], ε);
    }

    {
      // □□□□
      // ■□□□
      // □□□□
      // □□□□
      Double2D[] expected_2_0_1 = new Double2D[]{new Double2D(-139.036243, 0), new Double2D(-61.565051, 57.654332)};
      leaeE.tileBoundary(2, 0, 1, 0);
      Double2D[] result_2_0_1 = leaeE.tileBoundary(2, 0, 1, bufferInTiles);
      assertEquals("2/0/1 with buffer failed", expected_2_0_1[0], result_2_0_1[0], ε);
      assertEquals("2/0/1 with buffer failed", expected_2_0_1[1], result_2_0_1[1], ε);
    }

    {
      // □□□□
      // □□□□
      // □■□□
      // □□□□
      Double2D[] expected_2_1_2 = new Double2D[]{new Double2D(-80, 12.690523), new Double2D(10, 90)};
      leaeE.tileBoundary(2, 1, 2, 0);
      Double2D[] result_2_1_2 = leaeE.tileBoundary(2, 1, 2, bufferInTiles);
      assertEquals("2/1/2 with buffer failed", expected_2_1_2[0], result_2_1_2[0], ε);
      assertEquals("2/1/2 with buffer failed", expected_2_1_2[1], result_2_1_2[1], ε);
    }

    {
      // □□□□
      // □□□□
      // □□□□
      // □■□□
      Double2D[] expected_2_1_3 = new Double2D[]{new Double2D(-49.036243, 0), new Double2D(28.434949, 57.654332)};
      leaeE.tileBoundary(2, 1, 3, 0);
      Double2D[] result_2_1_3 = leaeE.tileBoundary(2, 1, 3, bufferInTiles);
      assertEquals("2/1/3 with buffer failed", expected_2_1_3[0], result_2_1_3[0], ε);
      assertEquals("2/1/3 with buffer failed", expected_2_1_3[1], result_2_1_3[1], ε);
    }

    {
      // □□□□
      // □□□□
      // □□□□
      // □□■□
      Double2D[] expected_2_2_3 = new Double2D[]{new Double2D(-8.434949, 0), new Double2D(69.036243, 57.654332)};
      leaeE.tileBoundary(2, 2, 3, 0);
      Double2D[] result_2_2_3 = leaeE.tileBoundary(2, 2, 3, bufferInTiles);
      assertEquals("2/2/3 with buffer failed", expected_2_2_3[0], result_2_2_3[0], ε);
      assertEquals("2/2/3 with buffer failed", expected_2_2_3[1], result_2_2_3[1], ε);
    }

    // Outside the projected area.
    {
      // ■□□□  □□□□
      // □□□□  □□□□
      // □□□□  □□□□
      // □□□□  □□□□
      //
      // □□□□  □□□□
      // □□□□  □□□□
      // □□□□  □□□□
      // □□□□  □□□□
      Double2D[] expected_3_0_0 = new Double2D[]{new Double2D(-137.094757, 0), new Double2D(-112.905243, 3.148974)};
      leaeE.tileBoundary(3, 0, 0, 0);
      Double2D[] result_3_0_0 = leaeE.tileBoundary(3, 0, 0, bufferInTiles);
      assertEquals("3/0/0 with buffer failed", expected_3_0_0[0], result_3_0_0[0], ε);
      assertEquals("3/0/0 with buffer failed", expected_3_0_0[1], result_3_0_0[1], ε);
    }

    // Very outside the projected area.
    {
      // ■□…
      // □…
      // …
      Double2D[] expected_18_0_0 = new Double2D[]{new Double2D(-125.000328, 0), new Double2D(-124.999672, 0)};
      leaeE.tileBoundary(18, 0, 0, 0);
      Double2D[] result_18_0_0 = leaeE.tileBoundary(18, 0, 0, bufferInTiles);
      assertEquals("18/0/0 with buffer failed", expected_18_0_0[0], result_18_0_0[0], ε);
      assertEquals("18/0/0 with buffer failed", expected_18_0_0[1], result_18_0_0[1], ε);
    }
  }
}
