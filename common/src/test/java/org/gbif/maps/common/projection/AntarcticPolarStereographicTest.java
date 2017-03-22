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
}
