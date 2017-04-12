package org.gbif.maps.common.projection;

import org.junit.Test;

import static org.junit.Assert.*;

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
}
