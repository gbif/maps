package org.gbif.maps.resource;

import org.gbif.maps.common.projection.Double2D;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SolrResourceTest {

  @Test
  public void testBufferedTileBoundary() {

    double buffer = 180 * SolrResource.SOLR_QUERY_BUFFER_PERCENTAGE;

    // Tile 0/0/0 and 0/1/0, no dateline
    // ■□
    Double2D[] expectedWest = new Double2D[]{new Double2D(-180 - buffer, -90), new Double2D(  0 + buffer, 90)};
    assertEquals("0/0/0 without dateline failed", expectedWest, SolrResource.bufferedTileBoundary(0, 0, 0));

    // □■
    Double2D[] expectedEast = new Double2D[]{new Double2D(   0 - buffer, -90), new Double2D(180 + buffer, 90)};
    assertEquals("0/1/0 without dateline failed", expectedEast, SolrResource.bufferedTileBoundary(0, 1, 0));

    // zoom 1, with dateline
    buffer /= 2;

    // ■□□□
    // □□□□
    Double2D[] expectedNW = new Double2D[]{new Double2D(-180 - buffer, 0 - buffer), new Double2D(-90 + buffer,         90)};
    assertEquals("1/0/0 with dateline failed", expectedNW, SolrResource.bufferedTileBoundary(1, 0, 0));

    // □□□□
    // □□□■
    Double2D[] expectedSE = new Double2D[]{new Double2D(  90 - buffer,        -90), new Double2D(180 + buffer, 0 + buffer)};
    assertEquals("1/3/3 with dateline failed", expectedSE, SolrResource.bufferedTileBoundary(1, 3, 1));
  }
}
