package org.gbif.maps.common.projection;

import org.junit.Test;

import static org.junit.Assert.*;

public class TilesTest {

  @Test
  public void testToTileLocalXY() {
    assertEquals(new Double2D(0,0), Tiles.toTileLocalXY(new Double2D(0,0), 1, 0, 0, 512, 32));
    assertEquals(new Double2D(10,522), Tiles.toTileLocalXY(new Double2D(522,522), 1, 1, 0, 512, 32));
    assertEquals(new Double2D(513,10), Tiles.toTileLocalXY(new Double2D(1025,10), 1, 1, 0, 512, 32)); // buffer
    assertEquals(new Double2D(-1,10), Tiles.toTileLocalXY(new Double2D(-1,10), 1, 0, 0, 512, 32)); // buffer
  }

  @Test
  public void testToTileLocalXYDateLine() {
    assertEquals(new Double2D(512,0), Tiles.toTileLocalXY(new Double2D(0,0), 1, 1, 0, 512, 32));
    assertEquals(new Double2D(1,10), Tiles.toTileLocalXY(new Double2D(1025,10), 1, 0, 0, 512, 32));
    assertEquals(new Double2D(510,10), Tiles.toTileLocalXY(new Double2D(-2,10), 1, 1, 0, 512, 32));
    assertEquals(new Double2D(514,10), Tiles.toTileLocalXY(new Double2D(2,10), 1, 1, 0, 512, 32));
  }

  @Test
  public void testToTileXY() {
    assertEquals(new Long2D(0,0), Tiles.toTileXY(new Double2D(0,0), 0, 512));
    assertEquals(new Long2D(1,1), Tiles.toTileXY(new Double2D(522,522), 1, 512));
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
