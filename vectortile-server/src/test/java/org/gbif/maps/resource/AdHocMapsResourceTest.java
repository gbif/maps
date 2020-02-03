package org.gbif.maps.resource;

import org.gbif.maps.common.projection.Double2D;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AdHocMapsResourceTest {

  @Test
  @Ignore
  public void testBufferedTileBoundary() {
    double bufferTile = Double.parseDouble(AdHocMapsResource.QUERY_BUFFER_PERCENTAGE);
    double buffer = 180 * bufferTile;

    // Tile 0/0/0 and 0/1/0, no dateline
    // ■□
    Double2D[] expectedWest = new Double2D[]{new Double2D(AdHocMapsResource.to180Degrees(-180 - buffer), -90), new Double2D(  0 + buffer, 90)};
    assertEquals("0/0/0 without dateline failed", expectedWest, AdHocMapsResource.bufferedTileBoundary(0, 0, 0, bufferTile));

    // □■
    Double2D[] expectedEast = new Double2D[]{new Double2D(   0 - buffer, -90), new Double2D(AdHocMapsResource.to180Degrees( 180 + buffer), 90)};
    assertEquals("0/1/0 without dateline failed", expectedEast, AdHocMapsResource.bufferedTileBoundary(0, 1, 0, bufferTile));

    // zoom 1, with dateline
    buffer /= 2;

    // ■□□□
    // □□□□
    Double2D[] expectedNW = new Double2D[]{new Double2D(AdHocMapsResource.to180Degrees(-180 - buffer), 0 - buffer), new Double2D(-90 + buffer,         90)};
    assertEquals("1/0/0 with dateline failed", expectedNW, AdHocMapsResource.bufferedTileBoundary(1, 0, 0, bufferTile));

    // □□□□
    // □□□■
    Double2D[] expectedSE = new Double2D[]{new Double2D(  90 - buffer,        -90), new Double2D(AdHocMapsResource.to180Degrees(180 + buffer), 0 + buffer)};
    assertEquals("1/3/3 with dateline failed", expectedSE, AdHocMapsResource.bufferedTileBoundary(1, 3, 1, bufferTile));
  }
}
