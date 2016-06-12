package org.gbif.maps.common.filter;

import org.gbif.maps.common.projection.Double2D;
import org.gbif.maps.common.projection.TileProjection;
import org.gbif.maps.common.projection.Tiles;
import org.gbif.maps.io.PointFeature;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.vividsolutions.jts.geom.Point;
import no.ecc.vectortile.VectorTileDecoder;
import no.ecc.vectortile.VectorTileEncoder;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

import static org.gbif.maps.io.PointFeature.PointFeatures.Feature.BasisOfRecord;

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
      1, 1, 0, 4096, 25,
      new Range(2010, 2011), Sets.newHashSet("OBSERVATION", "HUMAN_OBSERVATION"));

    // add all features in "layer2" which should all be ignored
    PointFeatureFilters.collectInVectorTile(
      encoder,
      "layer2",
      features,
      projection,
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
    assertTrue("lat[0], lng[-0.1] should result in pixel[-2,4096]", result.containsKey(new Double2D(-2,4096)));
    Map<String, Object> meta2 = result.get(new Double2D(-2,4096));
    assertEquals("Pixel 2 meta invalid - total", 1l, meta2.get("total"));
    assertEquals("Pixel 2 meta invalid - 2011", 1l, meta2.get("2011"));

    // pixel 3
    assertTrue("lat[10], lng[10] should result in pixel[228,3867]", result.containsKey(new Double2D(228,3867)));
    Map<String, Object> meta3 = result.get(new Double2D(228,3867));
    assertEquals("Pixel 3 meta invalid - total", 1l, meta3.get("total"));
    assertEquals("Pixel 3 meta invalid - 2011", 1l, meta3.get("2011"));
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
