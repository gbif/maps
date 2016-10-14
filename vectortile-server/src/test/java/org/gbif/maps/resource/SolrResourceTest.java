package org.gbif.maps.resource;

import org.gbif.maps.common.projection.Double2D;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SolrResourceTest {

  @Test
  public void testBufferedTileBoundary() {

    // zoom 0, no dateline should clip to world extent
    double buffer = 360 * SolrResource.SOLR_QUERY_BUFFER_PERCENTAGE;
    Double2D[] expected = new Double2D[]{new Double2D(-180, -90), new Double2D(180, 90)};
    assertEquals("0,0,0 without dateline failed", expected, SolrResource.bufferedTileBoundary(0, 0, 0, false));

    // zoom 1, with dateline
    buffer = 180 * SolrResource.SOLR_QUERY_BUFFER_PERCENTAGE;
    expected = new Double2D[]{new Double2D(180 - buffer, 0 - buffer), new Double2D(0 + buffer, 90)};
    assertEquals("1,0,0 with dateline failed", expected, SolrResource.bufferedTileBoundary(1, 0, 0, true));
  }


}
