package org.gbif.maps.common.projection;

import org.junit.Test;

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
  }
}
