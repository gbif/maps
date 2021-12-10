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
}
