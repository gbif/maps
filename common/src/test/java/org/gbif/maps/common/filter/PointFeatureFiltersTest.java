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
import org.gbif.maps.common.projection.TileProjection;
import org.gbif.maps.common.projection.Tiles;
import org.gbif.maps.io.PointFeature;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.vividsolutions.jts.geom.Point;

import no.ecc.vectortile.VectorTileDecoder;
import no.ecc.vectortile.VectorTileEncoder;

import static org.gbif.maps.common.projection.TileSchema.WEB_MERCATOR;
import static org.gbif.maps.common.projection.TileSchema.WGS84_PLATE_CAREÉ;
import static org.gbif.maps.io.PointFeature.PointFeatures.Feature.BasisOfRecord;
import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

/**
 * Only the methods that do any real logic are tested, as the majority of methods are simple wrappers around other
 * utilities (e.g. Range, Tiles etc) which cover their behavior in unit tests already.
 */
public class PointFeatureFiltersTest {

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

    // establish a test set of input
    List<PointFeature.PointFeatures.Feature> features = ImmutableList.of(
      newFeature(0.0, 0.0, 2009, BasisOfRecord.HUMAN_OBSERVATION, 1), // outside year range
      newFeature(0.0, 0.0, 2010, BasisOfRecord.HUMAN_OBSERVATION, 1), // in tile (pixel 1)
      newFeature(0.0, 0.0, 2010, BasisOfRecord.HUMAN_OBSERVATION, 1), // in tile (pixel 1) - A duplicate!
      newFeature(0.0, 0.0, 2011, BasisOfRecord.HUMAN_OBSERVATION, 1), // in tile (pixel 1)
      newFeature(0.0, 0.0, 2010, BasisOfRecord.OBSERVATION, 1), // in tile, new bor to combine (pixel 1)
      newFeature(0.0, 0.0, 2011, BasisOfRecord.OBSERVATION, 1), // in tile, new bor to combine (pixel 1)
      newFeature(0.0, 0.0, 2012, BasisOfRecord.OBSERVATION, 1), // outside year range
      newFeature(0.0, -0.1, 2011, BasisOfRecord.OBSERVATION, 1), // outside tile, but within buffer! (pixel 2)
      newFeature(10.0, 10.0, 2011, BasisOfRecord.OBSERVATION, 1), // in tile (pixel 3)
      newFeature(10.0, 10.0, 2011, BasisOfRecord.LIVING_SPECIMEN, 1), // outside bor range
      newFeature(-90.0, 0.0, 2011, BasisOfRecord.OBSERVATION, 1) // outside tile
    );

    // our test uses zoom=1, x=1, y=0 which is the NE quadrant of the world
    // add all features in "layer1"
    VectorTileEncoder encoder = new VectorTileEncoder(4096, 25, false);
    TileProjection projection = Tiles.fromEPSG("EPSG:3857", 4096); // Spherical Mercator
    PointFeatureFilters.collectInVectorTile(
      encoder,
      "layer1",
      features,
      projection,
      WEB_MERCATOR,
      1, 1, 0, 4096, 25,
      new Range(2010, 2011), Sets.newHashSet("OBSERVATION", "HUMAN_OBSERVATION"));

    // add all features in "layer2" which should all be ignored
    PointFeatureFilters.collectInVectorTile(
      encoder,
      "layer2",
      features,
      projection,
      WEB_MERCATOR,
      1, 1, 0, 4096, 25,
      new Range(2010, 2011), Sets.newHashSet("OBSERVATION", "HUMAN_OBSERVATION"));

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
    assertEquals("Expected 3 pixels of data", 3, result.size());

    // pixel 1
    assertTrue("lat[0], lng[0] should result in pixel[0,4096]", result.containsKey(new Double2D(0,4096)));
    Map<String, Object> meta1 = result.get(new Double2D(0,4096));
    assertEquals("Pixel 1 meta invalid - total", 5l, meta1.get("total"));
    assertEquals("Pixel 1 meta invalid - 2010", 3l, meta1.get("2010"));
    assertEquals("Pixel 1 meta invalid - 2011", 2l, meta1.get("2011"));

    // pixel 2
    assertTrue("lat[0], lng[-0.1] should result in pixel[-3,4096]", result.containsKey(new Double2D(-3,4096)));
    Map<String, Object> meta2 = result.get(new Double2D(-3,4096));
    assertEquals("Pixel 2 meta invalid - total", 1l, meta2.get("total"));
    assertEquals("Pixel 2 meta invalid - 2011", 1l, meta2.get("2011"));

    // pixel 3
    assertTrue("lat[10], lng[10] should result in pixel[227,3867]", result.containsKey(new Double2D(227,3867)));
    Map<String, Object> meta3 = result.get(new Double2D(227,3867));
    assertEquals("Pixel 3 meta invalid - total", 1l, meta3.get("total"));
    assertEquals("Pixel 3 meta invalid - 2011", 1l, meta3.get("2011"));
  }

  /**
   * A test to check that buffer regions address as we would expect.
   * This encodes some sample data into a tile at 1,1 in zoom 1 whereby some of the data will be in the buffer zone.
   * It then decodes it, verifying that the coordinates look as one would expect, and things outside of the buffer
   * were discarded.
   */
  @Test
  public void testBuffering() throws IOException {
    // our tile will be addressed at 1,1,1 (z,x,y), i.e.
    //   □□□□
    //   □■□□
    List<PointFeature.PointFeatures.Feature> features = ImmutableList.of(
      newFeature(  0.0, -90.0, 2009, BasisOfRecord.HUMAN_OBSERVATION, 1), // NW corner
      newFeature(  0.0,   0.0, 2010, BasisOfRecord.HUMAN_OBSERVATION, 1), // NE corner
      newFeature(-90.0, -90.0, 2009, BasisOfRecord.HUMAN_OBSERVATION, 1), // SW corner
      newFeature(-90.0,   0.0, 2010, BasisOfRecord.HUMAN_OBSERVATION, 1), // SE corner
      newFeature(-45.0, -91.0, 2010, BasisOfRecord.HUMAN_OBSERVATION, 1), // W bufferzone
      newFeature(  1.0, -45.0, 2010, BasisOfRecord.HUMAN_OBSERVATION, 1), // N bufferzone
      newFeature( 90.0,  45.0, 2010, BasisOfRecord.HUMAN_OBSERVATION, 1)  // Outside buffer
    );

    // add all features in "layer" which should all be ignored
    VectorTileEncoder encoder = new VectorTileEncoder(4096, 128, false);
    TileProjection projection = Tiles.fromEPSG("EPSG:4326", 4096); // Plate Carée (WGS84)
    PointFeatureFilters.collectInVectorTile(
      encoder,
      "bufferedLayer",
      features,
      projection,
      WGS84_PLATE_CAREÉ,
      1, 1, 1, 4096, 128,
      new Range(null, null), Sets.newHashSet()); // no filters
    byte[] encoded = encoder.encode();

    VectorTileDecoder decoder = new VectorTileDecoder();
    decoder.setAutoScale(false); // don't convert to 256 pixel tiles
    VectorTileDecoder.FeatureIterable featureIter = decoder.decode(encoded);

    Set<Double2D> coordinates = Sets.newHashSet();
    featureIter.forEach(f -> {
      assertTrue("Features must all be geometries", f.getGeometry() instanceof Point);
      Point p = (Point) f.getGeometry();
      Double2D pixel = new Double2D(p.getX(), p.getY());
      assertFalse("Tile should not have duplicate points", coordinates.contains(pixel));
      coordinates.add(pixel);
    });
    assertEquals("Expected 6 coordinates", 6, coordinates.size());
    assertTrue("NW missing", coordinates.contains(new Double2D(0,0)));
    assertTrue("NE missing", coordinates.contains(new Double2D(4096,0)));
    assertTrue("SW missing", coordinates.contains(new Double2D(0,4096)));
    assertTrue("SE missing", coordinates.contains(new Double2D(4096,4096)));

    // 4096.0/90.0° * -1° = -45.511, which is floored to -46.
    assertTrue("W buffer missing", coordinates.contains(new Double2D(-46,2048)));
    assertTrue("N buffer missing", coordinates.contains(new Double2D(2048,-46)));
  }


  /**
   * Utility to build features.
   */
  private static PointFeature.PointFeatures.Feature newFeature(
    double lat,
    double lng,
    int year,
    PointFeature.PointFeatures.Feature.BasisOfRecord bor,
    int count
  ) {
    return PointFeature.PointFeatures.Feature.newBuilder()
                                             .setLongitude(lng)
                                             .setLatitude(lat)
                                             .setYear(year)
                                             .setBasisOfRecord(bor)
                                             .setCount(count)
                                             .build();
  }
}
