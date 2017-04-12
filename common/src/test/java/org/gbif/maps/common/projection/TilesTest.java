package org.gbif.maps.common.projection;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.gbif.maps.common.projection.TileSchema.*;

public class TilesTest {

  @Test
  public void testToTileLocalXY() {
    assertEquals(new Double2D(  0,  0), Tiles.toTileLocalXY(new Double2D(   0,  0), WEB_MERCATOR, 1, 0, 0, 512, 32));
    assertEquals(new Double2D( 10,522), Tiles.toTileLocalXY(new Double2D( 522,522), WEB_MERCATOR, 1, 1, 0, 512, 32));
    assertEquals(new Double2D(513, 10), Tiles.toTileLocalXY(new Double2D(1025, 10), WEB_MERCATOR, 1, 1, 0, 512, 32)); // buffer
    assertEquals(new Double2D( -1, 10), Tiles.toTileLocalXY(new Double2D(  -1, 10), WEB_MERCATOR, 1, 0, 0, 512, 32)); // buffer

    assertEquals(new Double2D(100, 50), Tiles.toTileLocalXY(new Double2D( 612, 50), WEB_MERCATOR, 0, 1, 0, 512, 32));
  }

  @Test
  public void testToTileLocalXYDateLine() {
    // In Web Mercator, with one tile at zoom 0, extent at zoom 1 is 1024×1024
    assertEquals(new Double2D(512, 0), Tiles.toTileLocalXY(new Double2D(   0, 0), WEB_MERCATOR, 1, 1, 0, 512, 32)); // ▙
    assertEquals(new Double2D(  1,10), Tiles.toTileLocalXY(new Double2D(1025,10), WEB_MERCATOR, 1, 0, 0, 512, 32)); // ▟
    assertEquals(new Double2D(510,10), Tiles.toTileLocalXY(new Double2D(  -2,10), WEB_MERCATOR, 1, 1, 0, 512, 32)); // ▙
    assertEquals(new Double2D(514,10), Tiles.toTileLocalXY(new Double2D(   2,10), WEB_MERCATOR, 1, 1, 0, 512, 32)); // ▙

    // In a polar projection, with one tile at zoom 0, extent at zoom 1 is 1024×1024
    // and none of the edges are adjacent anyway
    assertEquals(new Double2D(-512, 0), Tiles.toTileLocalXY(new Double2D(   0, 0), POLAR, 1, 1, 0, 512, 32)); // ▙
    assertEquals(new Double2D(1025,10), Tiles.toTileLocalXY(new Double2D(1025,10), POLAR, 1, 0, 0, 512, 32)); // ▟
    assertEquals(new Double2D(-514,10), Tiles.toTileLocalXY(new Double2D(  -2,10), POLAR, 1, 1, 0, 512, 32)); // ▙
    assertEquals(new Double2D(-510,10), Tiles.toTileLocalXY(new Double2D(   2,10), POLAR, 1, 1, 0, 512, 32)); // ▙

    // In WGS84 Plate Careé, with two tiles at zoom 0, extent at zoom 1 is 2048×1024
    // Western hemisphere
    assertEquals(new Double2D(-512, 0), Tiles.toTileLocalXY(new Double2D(   0, 0), WGS84_PLATE_CAREÉ, 1, 1, 0, 512, 32)); // ▙█
    assertEquals(new Double2D(1025,10), Tiles.toTileLocalXY(new Double2D(1025,10), WGS84_PLATE_CAREÉ, 1, 0, 0, 512, 32)); // ▟█
    assertEquals(new Double2D(-514,10), Tiles.toTileLocalXY(new Double2D(  -2,10), WGS84_PLATE_CAREÉ, 1, 1, 0, 512, 32)); // ▙█
    assertEquals(new Double2D(-510,10), Tiles.toTileLocalXY(new Double2D(   2,10), WGS84_PLATE_CAREÉ, 1, 1, 0, 512, 32)); // ▙█
    // Eastern hemisphere
    assertEquals(new Double2D(512, 0), Tiles.toTileLocalXY(new Double2D(   0, 0), WGS84_PLATE_CAREÉ, 1, 3, 0, 512, 32)); // █▙
    assertEquals(new Double2D(  1,10), Tiles.toTileLocalXY(new Double2D(1025,10), WGS84_PLATE_CAREÉ, 1, 2, 0, 512, 32)); // █▟
    assertEquals(new Double2D(510,10), Tiles.toTileLocalXY(new Double2D(  -2,10), WGS84_PLATE_CAREÉ, 1, 3, 0, 512, 32)); // █▙
    assertEquals(new Double2D(514,10), Tiles.toTileLocalXY(new Double2D(   2,10), WGS84_PLATE_CAREÉ, 1, 3, 0, 512, 32)); // █▙
  }

  @Test
  public void testToTileXY() {
    assertEquals(new Long2D(0,0), Tiles.toTileXY(new Double2D(0,0), WEB_MERCATOR, 0, 512));
    assertEquals(new Long2D(1,1), Tiles.toTileXY(new Double2D(522,522), WEB_MERCATOR, 1, 512));

    assertEquals(new Long2D(0,0), Tiles.toTileXY(new Double2D(    100,50), WEB_MERCATOR, 0, 512));
    assertEquals(new Long2D(0,0), Tiles.toTileXY(new Double2D(512+100,50), WEB_MERCATOR, 0, 512)); // Wrapped

    // assertEquals(new Long2D(0,0), Tiles.toTileXY(new Double2D(512+100,50), POLAR, 0, 512)); // TODO! This is beyond the projection.

    assertEquals(new Long2D(0,0), Tiles.toTileXY(new Double2D(    100,50), WGS84_PLATE_CAREÉ, 0, 512));
    assertEquals(new Long2D(1,0), Tiles.toTileXY(new Double2D(512+100,50), WGS84_PLATE_CAREÉ, 0, 512));

    assertEquals(new Long2D( 8, 4), Tiles.toTileXY(new Double2D(4125.7777777777778,  2199.11111111111111), POLAR, 5, 512));
    assertEquals(new Long2D(18,19), Tiles.toTileXY(new Double2D(9625.7777777777778, 10199.11111111111111), POLAR, 5, 512));
  }

  @Test
  public void testTileContains() {
    assertTrue(Tiles.tileContains(1, 0, 0, 512, new Double2D(256,256), 0));
    assertTrue(Tiles.tileContains(1, 1, 1, 255, new Double2D(256,256), 0));
    assertTrue(Tiles.tileContains(1, 1, 1, 256, new Double2D(250,250), 10));
    assertFalse(Tiles.tileContains(1, 1, 1, 256, new Double2D(245,245), 10));
  }

  @Test
  public void testTileContainsDateline() {
    assertTrue(Tiles.tileContains(1, 1, 0, 512, new Double2D(10,10), 64)); // East buffer
    assertTrue(Tiles.tileContains(1, 0, 0, 512, new Double2D(1023,10), 64)); // West buffer
  }
}
