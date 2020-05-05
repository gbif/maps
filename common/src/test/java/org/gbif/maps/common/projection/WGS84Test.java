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

  @Test
  public void testTileBoundary() {
    WGS84 wgs84 = new WGS84(512);

    // Tile 0/0/0 and 0/1/0, no dateline
    // ■□
    Double2D[] expectedWest = new Double2D[]{new Double2D(-180, -90), new Double2D(  0, 90)};
    assertEquals("0/0/0 without dateline failed", expectedWest, wgs84.tileBoundary(0, 0, 0, 0));

    // □■
    Double2D[] expectedEast = new Double2D[]{new Double2D(   0, -90), new Double2D( 180, 90)};
    assertEquals("0/1/0 without dateline failed", expectedEast, wgs84.tileBoundary(0, 1, 0, 0));

    // zoom 1, with dateline

    // ■□□□
    // □□□□
    Double2D[] expectedNW = new Double2D[]{new Double2D(-180,   0), new Double2D(-90, 90)};
    assertEquals("1/0/0 with dateline failed", expectedNW, wgs84.tileBoundary(1, 0, 0, 0));

    // □□□□
    // □□□■
    Double2D[] expectedSE = new Double2D[]{new Double2D(  90, -90), new Double2D(180,  0)};
    assertEquals("1/3/1 with dateline failed", expectedSE, wgs84.tileBoundary(1, 3, 1, 0));

    double bufferInTiles = 0.25;

    Double2D[] expectedWestBuf = new Double2D[]{new Double2D(135, -90), new Double2D(45, 90)};
    assertEquals("0/0/0 with dateline failed", expectedWestBuf, wgs84.tileBoundary(0, 0, 0, bufferInTiles));

    // □□□□
    // □□□■
    Double2D[] expectedSEBuf = new Double2D[]{new Double2D(67.5, -90), new Double2D(-157.5, 22.5)};
    assertEquals("1/3/1 with dateline failed", expectedSEBuf, wgs84.tileBoundary(1, 3, 1, bufferInTiles));
  }
}
