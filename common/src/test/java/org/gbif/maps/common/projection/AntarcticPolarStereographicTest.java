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

import static org.junit.AssertOnDouble2D.assertEquals;

public class AntarcticPolarStereographicTest {

  static double ε = 1e-5;

  @Test
  public void testToGlobalPixelXY() {
    WGS84AntarcticPolarStereographic aps = new WGS84AntarcticPolarStereographic(512);

    // Antarctic Polar Stereographic is orientated such that 0° latitude extends upwards from the centre of the map.

    // Zoom 0 has one tile, 512×512, 1×1, giving a total area of 512×512.
    assertEquals(new Double2D( 256,  256), aps.toGlobalPixelXY( -90,   0, 0), ε); // Map centre
    assertEquals(new Double2D(   0,  256), aps.toGlobalPixelXY(   0, -90, 0), ε); // Left edge centre
    assertEquals(new Double2D( 512,  256), aps.toGlobalPixelXY(   0,  90, 0), ε); // Right edge centre
    assertEquals(new Double2D( 256,    0), aps.toGlobalPixelXY(   0,   0, 0), ε); // Top edge centre
    assertEquals(new Double2D( 256,  512), aps.toGlobalPixelXY(   0, 180, 0), ε); // Bottom edge centre

    // Zoom 2 has 2ⁿ⁺¹ = 2³ = 16 tiles, 4×4, giving a total area of 2048×2048.
    assertEquals(new Double2D(1024, 1024), aps.toGlobalPixelXY( -90,   0, 2), ε); // Map centre
    assertEquals(new Double2D(   0, 1024), aps.toGlobalPixelXY(   0, -90, 2), ε); // Left edge centre
    assertEquals(new Double2D(2048, 1024), aps.toGlobalPixelXY(   0,  90, 2), ε); // Right edge centre
    assertEquals(new Double2D(1024,    0), aps.toGlobalPixelXY(   0,   0, 2), ε); // Top edge centre
    assertEquals(new Double2D(1024, 2048), aps.toGlobalPixelXY(   0, 180, 2), ε); // Bottom edge centre
  }

  @Test
  public void testTileBoundary() {
    WGS84AntarcticPolarStereographic aps = new WGS84AntarcticPolarStereographic(1024);

    {
      // Tile 0/0/0
      // ■
      Double2D[] expected = new Double2D[]{new Double2D(-180, -90), new Double2D(180, 0)};
      Double2D[] result = aps.tileBoundary(0, 0, 0, 0);
      assertEquals("0/0/0 failed", expected[0], result[0], ε);
      assertEquals("0/0/0 failed", expected[1], result[1], ε);
    }

    // zoom 1
    {
      // ■□
      // □□
      Double2D[] expected_1_0_0 = new Double2D[]{new Double2D(-90, -90), new Double2D(0, 0)};
      Double2D[] result_1_0_0 = aps.tileBoundary(1, 0, 0, 0);
      assertEquals("1/0/0 failed", expected_1_0_0[0], result_1_0_0[0], ε);
      assertEquals("1/0/0 failed", expected_1_0_0[1], result_1_0_0[1], ε);
    }
    {
      // □■
      // □□
      Double2D[] expected_1_1_0 = new Double2D[]{new Double2D(0, -90), new Double2D(90, 0)};
      Double2D[] result_1_1_0 = aps.tileBoundary(1, 1, 0, 0);
      assertEquals("1/1/0 failed", expected_1_1_0[0], result_1_1_0[0], ε);
      assertEquals("1/1/0 failed", expected_1_1_0[1], result_1_1_0[1], ε);
    }
    {
      // □□
      // ■□
      Double2D[] expected_1_0_1 = new Double2D[]{new Double2D(-180, -90), new Double2D(-90, 0)};
      Double2D[] result_1_0_1 = aps.tileBoundary(1, 0, 1, 0);
      assertEquals("1/0/1 failed", expected_1_0_1[0], result_1_0_1[0], ε);
      assertEquals("1/0/1 failed", expected_1_0_1[1], result_1_0_1[1], ε);
    }
    {
      // □□
      // □■
      Double2D[] expected_1_1_1 = new Double2D[]{new Double2D(90, -90), new Double2D(180, 0)};
      Double2D[] result_1_1_1 = aps.tileBoundary(1, 1, 1, 0);
      assertEquals("1/1/1 failed", expected_1_1_1[0], result_1_1_1[0], ε);
      assertEquals("1/1/1 failed", expected_1_1_1[1], result_1_1_1[1], ε);
    }

    // zoom 2
    {
      // □□□□
      // □■□□
      // □□□□
      // □□□□
      Double2D[] expected_2_1_2 = new Double2D[]{new Double2D(-90, -90), new Double2D(0, -19.592468)};
      Double2D[] result_2_1_1 = aps.tileBoundary(2, 1, 1, 0);
      assertEquals("2/1/1 failed", expected_2_1_2[0], result_2_1_1[0], ε);
      assertEquals("2/1/1 failed", expected_2_1_2[1], result_2_1_1[1], ε);
    }
    {
      // □□□□
      // □□□■
      // □□□□
      // □□□□
      Double2D[] expected_2_3_1 = new Double2D[]{new Double2D(45, -37.054722), new Double2D(90, 0)};
      Double2D[] result_2_3_1 = aps.tileBoundary(2, 3, 1, 0);
      assertEquals("2/3/1 failed", expected_2_3_1[0], result_2_3_1[0], ε);
      assertEquals("2/3/1 failed", expected_2_3_1[1], result_2_3_1[1], ε);
    }
    {
      // □□□□
      // □□□□
      // □□□□
      // □■□□
      Double2D[] expected_2_1_3 = new Double2D[]{new Double2D(-180, -37.054722), new Double2D(-135, 0)};
      Double2D[] result_2_1_3 = aps.tileBoundary(2, 1, 3, 0);
      assertEquals("2/1/3 failed", expected_2_1_3[0], result_2_1_3[0], ε);
      assertEquals("2/1/3 failed", expected_2_1_3[1], result_2_1_3[1], ε);
    }
    {
      // □□□□
      // □□□□
      // □□□□
      // □□□■
      Double2D[] expected_2_3_3 = new Double2D[]{new Double2D(116.565051, -19.592468), new Double2D(153.4349488, 0)};
      Double2D[] result_2_3_3 = aps.tileBoundary(2, 3, 3, 0);
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
      Double2D[] expected3_5_3 = new Double2D[]{new Double2D(45, -62.08687), new Double2D(90, -31.76006)};
      Double2D[] result3_5_3 = aps.tileBoundary(3, 5, 3, 0);
      assertEquals("3/5/3 failed", expected3_5_3[0], result3_5_3[0], ε);
      assertEquals("3/5/3 failed", expected3_5_3[1], result3_5_3[1], ε);
    }

    double bufferInTiles = 0.25;

    {
      // Tile 0/0/0
      // ■
      Double2D[] expectedBuf = new Double2D[]{new Double2D(-180, -90), new Double2D(180, 0)};
      Double2D[] resultBuf = aps.tileBoundary(0, 0, 0, bufferInTiles);
      assertEquals("0/0/0 with buffer failed", expectedBuf[0], resultBuf[0], ε);
      assertEquals("0/0/0 with buffer failed", expectedBuf[1], resultBuf[1], ε);
    }

    {
      // □□
      // □■
      Double2D[] expected_1_1_1 = new Double2D[]{new Double2D(90, -90), new Double2D(180, 0)};
      aps.tileBoundary(1, 1, 1, 0);
      Double2D[] result_1_1_1 = aps.tileBoundary(1, 1, 1, bufferInTiles);
      assertEquals("1/1/1 with buffer failed", expected_1_1_1[0], result_1_1_1[0], ε);
      assertEquals("1/1/1 with buffer failed", expected_1_1_1[1], result_1_1_1[1], ε);
    }

    {
      // □□□□
      // ■□□□
      // □□□□
      // □□□□
      Double2D[] expected_2_0_1 = new Double2D[]{new Double2D(-108.4349488229220, -47.0556006), new Double2D(-30.96375653207352, 0)};
      aps.tileBoundary(2, 0, 1, 0);
      Double2D[] result_2_0_1 = aps.tileBoundary(2, 0, 1, bufferInTiles);
      assertEquals("2/0/1 with buffer failed", expected_2_0_1[0], result_2_0_1[0], ε);
      assertEquals("2/0/1 with buffer failed", expected_2_0_1[1], result_2_0_1[1], ε);
    }

    {
      // □□□□
      // □□□□
      // □■□□
      // □□□□
      Double2D[] expected_2_1_2 = new Double2D[]{new Double2D(-180, -90), new Double2D(-90, -7.101188115321475)};
      aps.tileBoundary(2, 1, 2, 0);
      Double2D[] result_2_1_2 = aps.tileBoundary(2, 1, 2, bufferInTiles);
      assertEquals("2/1/2 with buffer failed", expected_2_1_2[0], result_2_1_2[0], ε);
      assertEquals("2/1/2 with buffer failed", expected_2_1_2[1], result_2_1_2[1], ε);
    }

    {
      // □□□□
      // □□□□
      // □□□□
      // □■□□
      Double2D[] expected_2_1_3 = new Double2D[]{new Double2D(161.56505117707798, -47.0556006), new Double2D(-120.96375653207352, 0)};
      aps.tileBoundary(2, 1, 3, 0);
      Double2D[] result_2_1_3 = aps.tileBoundary(2, 1, 3, bufferInTiles);
      assertEquals("2/1/3 with buffer failed", expected_2_1_3[0], result_2_1_3[0], ε);
      assertEquals("2/1/3 with buffer failed", expected_2_1_3[1], result_2_1_3[1], ε);
    }

    {
      // □□□□
      // □□□□
      // □□□□
      // □□■□
      Double2D[] expected_2_2_3 = new Double2D[]{new Double2D(120.96375653207352, -47.0556006), new Double2D(-161.56505117707798, 0)};
      aps.tileBoundary(2, 2, 3, 0);
      Double2D[] result_2_2_3 = aps.tileBoundary(2, 2, 3, bufferInTiles);
      assertEquals("2/2/3 with buffer failed", expected_2_2_3[0], result_2_2_3[0], ε);
      assertEquals("2/2/3 with buffer failed", expected_2_2_3[1], result_2_2_3[1], ε);
    }
  }
}
