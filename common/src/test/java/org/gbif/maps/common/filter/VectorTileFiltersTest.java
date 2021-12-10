/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.maps.common.filter;

import org.gbif.maps.common.projection.Double2D;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

import no.ecc.vectortile.VectorTileDecoder;
import no.ecc.vectortile.VectorTileEncoder;

import static org.gbif.maps.common.projection.TileSchema.WEB_MERCATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Only the methods that do any real logic are tested, as the majority of methods are simple wrappers around other
 * utilities (e.g. Range, Tiles etc) which cover their behavior in unit tests already.
 */
public class VectorTileFiltersTest {
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  /**
   * Tests typical behaviour of all filters in combination.
   * Specifically this verifies basic behaviour of:
   * <ul>
   *   <li>Basis of record layers are correctly flattened into a single layer</li>
   *   <li>Year ranges are applied</li>
   *   <li>Buffered areas are written in to</li>
   *   <li>Counts are accumulated correctly across basis of record layers when flattened</li>
   *   <li>Duplicate features are accumulated, even though they are not expected</li>
   *   <li>Decoding applies the layer filter</li>
   * </ul>
   */
  @Test
  public void testCollectInVectorTile() throws IOException {

    // establish test input
    VectorTileEncoder encoder = new VectorTileEncoder(512, 0, false); // no buffer (like our HBase tiles)
    Double2D px1 = new Double2D(10,10); // on tile
    Double2D px2 = new Double2D(255,255); // middle-ish of tile
    Double2D px3 = new Double2D(511,50); // at edge of tile to test buffering
    Map<String, Object> meta1 = ImmutableMap.<String,Object>of("2012", 1, "2013", 2); // matches our filter year range
    Map<String, Object> meta2 = ImmutableMap.<String,Object>of("2011", 1); // outside year range

    collect(encoder, "observation", px1.getX(), px1.getY(), meta1);
    collect(encoder, "observation", px1.getX(), px1.getY(), meta1); // duplicate feature
    collect(encoder, "specimen", px1.getX(), px1.getY(), meta1); // same feature, different layer to include
    collect(encoder, "livingSpecimen", px1.getX(), px1.getY(), meta1); // same feature, different layer to exclude
    collect(encoder, "observation", px1.getX(), px1.getY(), meta2); // outside year range
    collect(encoder, "observation", px2.getX(), px2.getY(), meta1); // a second feature location (tile middle)
    collect(encoder, "observation", px3.getX(), px3.getY(), meta1); // a third feature (tile edge)

    // These tiles are equivalent to what would come from HBase as a precalculated tile with no buffer
    byte[] sourceTile = encoder.encode();

    encoder = new VectorTileEncoder(512, 25, false); // New encoder with a buffer to collect into

    // our test uses zoom=1, x=1, y=0 which is the NE quadrant of the world
    // add all features in "layer1"
    Set<String> bors = ImmutableSet.of("observation", "specimen");
    Range years = new Range(2012, 2013);
    VectorTileFilters.collectInVectorTile(encoder, "layer1", sourceTile, WEB_MERCATOR, 1, 1, 0, 1, 0, 512, 25, years, bors, true);

    // collect the data into "layer1" but this time indicating it comes from a tile that is the
    // NW quadrant of the world
    VectorTileFilters.collectInVectorTile(encoder, "layer1", sourceTile, WEB_MERCATOR, 1, 1, 0, 0, 0, 512, 25, years, bors, true);

    // collect into "layer2" which should be ignored completely
    VectorTileFilters.collectInVectorTile(encoder, "layer2", sourceTile, WEB_MERCATOR, 1, 1, 0, 1, 0, 512, 25, years, bors, true);


    // build the vector tile to test the output
    byte[] encoded = encoder.encode();
    VectorTileDecoder decoder = new VectorTileDecoder();
    decoder.setAutoScale(false); // don't convert to 256 pixel tiles
    VectorTileDecoder.FeatureIterable featureIter = decoder.decode(encoded, "layer1"); // only layer1

    // verify the results
    Map<Double2D, Map<String, Object>> result = Maps.newHashMap();
    featureIter.forEach(f -> {
      assertEquals("Only 1 layer named 'layer1' is expected", "layer1", f.getLayerName());
      assertTrue("Features must all be geometries", f.getGeometry() instanceof Point);
      Point p = (Point) f.getGeometry();
      Double2D pixel = new Double2D(p.getX(), p.getY());
      assertFalse("Tile should not have duplicate points", result.containsKey(pixel));
      result.put(pixel, f.getAttributes());
    });
    assertEquals("Expected 5 pixels of data", 5, result.size());

    // pixel 1
    assertTrue("Pixel 1 missing", result.containsKey(px1));
    Map<String, Object> m1 = result.get(px1);
    assertEquals("Pixel 1 meta invalid - total", 9l, m1.get("total"));
    assertEquals("Pixel 1 meta invalid - 2012", 3l, m1.get("2012"));
    assertEquals("Pixel 1 meta invalid - 2013", 6l, m1.get("2013"));

    // pixel 2
    assertTrue("Pixel 2 missing", result.containsKey(px2));
    Map<String, Object> m2 = result.get(px2);
    assertEquals("Pixel 2 meta invalid - total", 3l, m2.get("total"));
    assertEquals("Pixel 2 meta invalid - 2012", 1l, m2.get("2012"));
    assertEquals("Pixel 2 meta invalid - 2013", 2l, m2.get("2013"));

    // pixel 3
    assertTrue("Pixel 3 missing", result.containsKey(px3));
    Map<String, Object> m3 = result.get(px3);
    assertEquals("Pixel 3 meta invalid - total", 3l, m3.get("total"));
    assertEquals("Pixel 3 meta invalid - 2012", 1l, m3.get("2012"));
    assertEquals("Pixel 3 meta invalid - 2013", 2l, m3.get("2013"));

    // pixel 4 (comes in to buffer zone from the NW quadrant tile)
    Double2D px4 = new Double2D(-1,50);
    assertTrue("Pixel 4 missing (the buffer pixel)", result.containsKey(px4));
    Map<String, Object> m4 = result.get(px4);
    assertEquals("Pixel 4 meta invalid - total", 3l, m4.get("total"));
    assertEquals("Pixel 4 meta invalid - 2012", 1l, m4.get("2012"));
    assertEquals("Pixel 4 meta invalid - 2013", 2l, m4.get("2013"));

    // pixel 5 (comes in to buffer zone due to dateline wrapping)
    // pixel 5 comes from the px1 (10,10) on the tile at 0,0 which wraps across into our Eastern buffer
    Double2D px5 = new Double2D(522,10);
    assertTrue("Pixel 5 missing (the dateline pixel)", result.containsKey(px5));
    Map<String, Object> m5 = result.get(px5);
    assertEquals("Pixel 5 meta invalid - total", 9l, m5.get("total"));
    assertEquals("Pixel 5 meta invalid - 2012", 3l, m5.get("2012"));
    assertEquals("Pixel 5 meta invalid - 2013", 6l, m5.get("2013"));

  }

  /**
   * Utility to add features.
   */
  private static void collect(
    VectorTileEncoder encoder,
    String bor,
    double x,
    double y,
    Map<String, Object> meta
  ) {
    encoder.addFeature(bor, meta, GEOMETRY_FACTORY.createPoint(new Coordinate(x,y)));
  }
}
