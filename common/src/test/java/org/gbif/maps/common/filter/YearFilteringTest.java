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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import com.google.common.io.Files;
import com.google.common.io.Resources;

import no.ecc.vectortile.VectorTileDecoder;
import no.ecc.vectortile.VectorTileEncoder;

import static org.gbif.maps.common.projection.TileSchema.WGS84_PLATE_CAREÉ;
import static org.junit.Assert.assertEquals;

/**
 * Test that a tile from HBase filters correctly when a year range is applied.
 * See https://github.com/gbif/maps/issues/48.
 */
public class YearFilteringTest {
  /**
   * Using a raw tile from HBase, verify that the total count of the year matches that when it is filtered
   */
  @Test
  public void testSingleYearFilter() throws IOException {
    // a real tile extracted using the ExportRawTile utility from HBase (9th Dec 2021)
    byte[] sourceTile = Resources.toByteArray(Resources.getResource("tiles/publishingCountry-FR-3-8-2.mvt"));

    // an unfiltered tile in verbose mode (has values on all the years)
    VectorTileEncoder encoder = new VectorTileEncoder(512, 25, false);
    VectorTileFilters.collectInVectorTile(encoder, "occurrence", sourceTile, WGS84_PLATE_CAREÉ, 3, 8, 2, 8, 2, 512, 25, Range.UNBOUNDED, null, true);
    byte[] unfiltered = encoder.encode();

    // a tile filtered only for the 2018 data (no need for verbose)
    encoder = new VectorTileEncoder(512, 25, false);
    Range years = new Range(2018, 2018);
    VectorTileFilters.collectInVectorTile(encoder, "occurrence", sourceTile, WGS84_PLATE_CAREÉ, 3, 8, 2, 8, 2, 512, 25, years, null, false);
    byte[] filtered = encoder.encode();

    // extract the feature and total count of 2018 from the unfiltered tile, and the one prefiltered
    long[] unfilteredSummary = featureAndTotalCount(unfiltered, "2018");
    long[] filteredSummary = featureAndTotalCount(filtered, "total");

    assertEquals("Feature count of filtered should equal unfiltered", unfilteredSummary[0], filteredSummary[0]);
    assertEquals("Total of filtered should equal unfiltered", unfilteredSummary[1], filteredSummary[1]);

    Files.write(filtered, new File("/tmp/filtered.mvt"));
    Files.write(unfiltered, new File("/tmp/unfiltered.mvt"));
  }

  /**
   * Extract the feature count, and total from the provided tile
   */
  long[] featureAndTotalCount(byte[] tile, String featureName) throws IOException {
    VectorTileDecoder decoder = new VectorTileDecoder();
    decoder.setAutoScale(false); // don't convert to 256 pixel tiles
    List<VectorTileDecoder.Feature> featureList = decoder.decode(tile, "occurrence").asList();
    AtomicLong total = new AtomicLong(0);
    AtomicLong featureCount = new AtomicLong(0);
    featureList.forEach(f -> {
      Map<String, Object> features = f.getAttributes();
      if (features.containsKey(featureName)) {
        featureCount.incrementAndGet();
        total.getAndAdd((Long) features.get(featureName));
      }
    });

    return new long[]{ featureCount.get(), total.get() };
  }

}
