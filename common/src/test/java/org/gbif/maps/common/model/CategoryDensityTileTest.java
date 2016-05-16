package org.gbif.maps.common.model;

import org.junit.Test;

import static org.junit.Assert.*;

public class CategoryDensityTileTest {

  // Test basic functionality
  @Test
  public void testEncodeDecodeKey() {
    short x = 4012;
    short y = 3213;
    short category = 14;
    short year = 2011;
    long encoded = CategoryDensityTile.encodeKey(x,y,category,year);
    short[] decoded = CategoryDensityTile.decodeKey(encoded);
    assertEquals("X decoded incorrectly", x, decoded[0]);
    assertEquals("Y decoded incorrectly", y, decoded[1]);
    assertEquals("Category decoded incorrectly", category, decoded[2]);
    assertEquals("Year decoded incorrectly", year, decoded[3]);
  }

  @Test
  public void testCollectAll() {
    CategoryDensityTile t1 = new CategoryDensityTile();
    t1.collect(0,0,0,2000,1);
    t1.collect(0,0,0,2001,1);
    t1.collect(0,1,0,2000,1);

    CategoryDensityTile t2 = new CategoryDensityTile();
    t2.collect(0,0,0,2000,1);
    t2.collect(0,0,0,2001,2);
    t2.collect(0,1,0,2000,1);
    t2.collect(1,1,1,2000,1);

    String expected = "0,0,0,2000,2\\n"
                      + "0,0,0,2001,3\\n"
                      + "0,1,0,2000,2\\n"
                      + "1,1,1,2000,1\\n";

    assertEquals("Collecting not as expected", expected, t1.collectAll(t2).toDebugString());
  }

  @Test
  public void testDownscale() {
    CategoryDensityTile t1 = new CategoryDensityTile();
    t1.collect(10,0,0,2000,1);
    t1.collect(11,11,0,2001,1);
    t1.collect(21,3,0,2000,1);

    CategoryDensityTile t2 = t1.downscale(1, 1, 0, 4096);

    String expected = "2053,0,0,2000,1\\n"
                      + "2053,5,0,2001,1\\n"
                      + "2058,1,0,2000,1\\n";

    assertEquals("Downscaling not as expected", expected, t2.toDebugString());
  }
}
