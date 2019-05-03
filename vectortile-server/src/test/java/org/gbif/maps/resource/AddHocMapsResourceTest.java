package org.gbif.maps.resource;

import org.gbif.maps.common.projection.Double2D;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AddHocMapsResourceTest {

  @Test
  public void testBufferedTileBoundary() {

    double buffer = 180 * AddHocMapsResource.QUERY_BUFFER_PERCENTAGE;

    // Tile 0/0/0 and 0/1/0, no dateline
    // ■□
    Double2D[] expectedWest = new Double2D[]{new Double2D(AddHocMapsResource.to180Degrees(-180 - buffer), -90), new Double2D(  0 + buffer, 90)};
    assertEquals("0/0/0 without dateline failed", expectedWest, AddHocMapsResource.bufferedTileBoundary(0, 0, 0));

    // □■
    Double2D[] expectedEast = new Double2D[]{new Double2D(   0 - buffer, -90), new Double2D(AddHocMapsResource.to180Degrees( 180 + buffer), 90)};
    assertEquals("0/1/0 without dateline failed", expectedEast, AddHocMapsResource.bufferedTileBoundary(0, 1, 0));

    // zoom 1, with dateline
    buffer /= 2;

    // ■□□□
    // □□□□
    Double2D[] expectedNW = new Double2D[]{new Double2D(AddHocMapsResource.to180Degrees(-180 - buffer), 0 - buffer), new Double2D(-90 + buffer,         90)};
    assertEquals("1/0/0 with dateline failed", expectedNW, AddHocMapsResource.bufferedTileBoundary(1, 0, 0));

    // □□□□
    // □□□■
    Double2D[] expectedSE = new Double2D[]{new Double2D(  90 - buffer,        -90), new Double2D(AddHocMapsResource.to180Degrees(180 + buffer), 0 + buffer)};
    assertEquals("1/3/3 with dateline failed", expectedSE, AddHocMapsResource.bufferedTileBoundary(1, 3, 1));
  }
}
