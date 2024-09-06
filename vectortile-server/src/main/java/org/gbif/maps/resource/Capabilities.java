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
package org.gbif.maps.resource;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.gbif.maps.common.projection.Double2D;
import org.gbif.maps.common.projection.Int2D;

import java.io.IOException;
import java.time.Year;
import java.util.Arrays;
import java.util.Map;

import org.slf4j.logger;
import org.slf4j.loggerFactory;

import com.carrotsearch.hppc.IntHashSet;
import com.google.common.annotations.VisibleForTesting;
import com.vividsolutions.jts.geom.Point;

import io.swagger.v3.oas.annotations.media.Schema;
import no.ecc.vectortile.VectorTileDecoder;

/**
 * The model map capability responses intended to be used for web clients to initialize map widgets for display.
 * This encapsulates the nuisance of determining the extent.
 *
 * Returned values are accurate to the nearest degree, since this process typically uses zoom level 0 tiles, and
 * that is roughly the limit of the resolution.
 */
@Slf4j
@Getter
public class Capabilities {

  private static final String META_TOTAL = "total";

  @Schema(description = "The lowest latitude of occurrences covered by the query")
  private final Double minLat;

  @Schema(description = "The highest latitude of occurrences covered by the query")
  private final Double maxLat;

  @Schema(
    description = "The lowest longitude of occurrences covered by the query.\n\n" +
      "For clusters of occurrences centered around the antimeridian (180° longitude) the `minLng` and `maxLng` " +
      "will have a value just below 180° and above 180°."
  )
  private final Double minLng;

  @Schema(
    description = "The highest longitude of occurrences covered by the query.\n\n" +
      "For clusters of occurrences centered around the antimeridian (180° longitude) the `minLng` and `maxLng` " +
      "will have a value just below 180° and above 180°."
  )
  private final Double maxLng;

  @Schema(description = "The lowest year of occurrences covered by the query.")
  private final Integer minYear;

  @Schema(description = "The highest year of occurrences covered by the query.")
  private final Integer maxYear;

  @Schema(description = "The number of occurrences covered by the whole map.")
  private final long total;

  @Schema(
    description = "The time the map tile was generated.  Tiles are cached for hours to days, depending on the " +
      "processing required to produce them."
  )
  private final String generated;

  private Capabilities(
    Double minLat,
    Double minLng,
    Double maxLat,
    Double maxLng,
    Integer minYear,
    Integer maxYear,
    long total,
    String generated
  ) {
    this.minLat = minLat;
    this.minLng = minLng;
    this.maxLat = maxLat;
    this.maxLng = maxLng;
    this.minYear = minYear;
    this.maxYear = maxYear;
    this.total = total;
    this.generated = generated;
  }

  public int getMinLat() {
    return floor(minLat, -90);
  }

  public int getMinLng() {
    return floor(minLng, -180);
  }

  public int getMaxLat() {
    return ceil(maxLat, 90);
  }

  public int getMaxLng() {
    return ceil(maxLng, 180);
  }

  private static int floor(Double d, int defaultValue) {
    if (d == null) return defaultValue;
    else return (int) Math.floor(d);
  }

  private static int ceil(Double d, int defaultValue) {
    if (d == null) return defaultValue;
    else return (int) Math.ceil(d);
  }

  @Override
  public String toString() {
    // returns the internal values which are rounded appropriately on the retrieval in getters
    return "Capabilities{" +
           "minLat=" + minLat +
           ", minLng=" + minLng +
           ", maxLat=" + maxLat +
           ", maxLng=" + maxLng +
           ", minYear=" + minYear +
           ", maxYear=" + maxYear +
           ", total=" + total +
           ", generated=" + generated +
           '}';
  }


  /**
   * A builder of the capabilities produced by inspecting vector tile data.
   */
  static class CapabilitiesBuilder {
    private static final VectorTileDecoder DECODER = new VectorTileDecoder();
    private static final int CURRENT_YEAR = Year.now().getValue();
    private static final int MIN_YEAR = 1600; // reasonable limit on sliders

    private double minLat = Double.NaN, minLng = Double.NaN;
    private double maxLat = Double.NaN, maxLng = Double.NaN;
    private int minYear = Integer.MAX_VALUE, maxYear = Integer.MIN_VALUE;
    private long total;
    private String generated;

    private IntHashSet longitudes = new IntHashSet();
    private int spreadMultiplier = 512; // Calculate spread after undoing division by tile extent, to avoid rounding issues.

    static {
      DECODER.setAutoScale(false); // important to avoid auto scaling to 256 tiles
    }

    static CapabilitiesBuilder newBuilder() {
      return new CapabilitiesBuilder();
    }

    Capabilities build() {
      int[] spread = centredSpread(longitudes.toArray(), 360*spreadMultiplier);
      int offset = Double.isNaN(minLat) ? 0 : (spread[0] > spread[1]) ? 360*spreadMultiplier : 0;

      // defensively code for empty tiles
      return new Capabilities(
        Double.isNaN(minLat) ? null : minLat,
        Double.isNaN(minLng) ? null : ((double)spread[0])/spreadMultiplier, // rather than minLng
        Double.isNaN(maxLat) ? null : maxLat,
        Double.isNaN(maxLng) ? null : ((double)(spread[1]+offset))/spreadMultiplier, // rather than maxLng
        Integer.MAX_VALUE == minYear ? null : minYear,
        Integer.MIN_VALUE == maxYear ? null : maxYear,
        total,
        generated
      );
    }

    /**
     * Collects a tile mutating the collected metadata accordingly.
     *
     * @param tile To inspect which is assumed to be in EPSG:4326 projection and must be Point data only
     * @param tileNW The lat,lng of the tile's North West
     * @param tileSE The lat,lng of the tile's South East
     * @throws IOException On encoding issues only
     */
    void collect(byte[] tile, Double2D tileNW, Double2D tileSE, String date) throws IOException {
      if (tile != null) { // defensive coding

        double minX = Double.MAX_VALUE,  minY = Double.MAX_VALUE;
        double maxX = -Double.MAX_VALUE, maxY = -Double.MAX_VALUE;
        double extent = -1;

        for (VectorTileDecoder.Feature feature : DECODER.decode(tile)) {
          extent = feature.getExtent(); // same for all, but only way to access in the API
          Point p = ((Point) feature.getGeometry());

          // skip data residing in tile buffers
          if (p.getX() >= 0 && p.getX() < extent && p.getY() >= 0 && p.getY() < extent) {

            // capture extent of range
            minX = Math.min(p.getX(), minX);
            minY = Math.min(p.getY(), minY);
            maxX = Math.max(p.getX(), maxX);
            maxY = Math.max(p.getY(), maxY);

            double lng = tileNW.getX() + (tileSE.getX() - tileNW.getX()) * p.getX() / extent;
            longitudes.add((int) Math.round(lng * spreadMultiplier));

            total += extractInt(feature.getAttributes(), META_TOTAL, 0);

            // optimisation
            if (!(minYear == MIN_YEAR && maxYear == CURRENT_YEAR)) {
              Int2D years = extractYear(feature.getAttributes());
              if (years != null) {
                minYear = Math.min(minYear, years.getX());
                maxYear = Math.max(maxYear, years.getY());
              }
            }
          }
        }

        // if we found data on the tile, adjust the bounds
        if (extent != -1) {
          // project the tile local space into real world coords
          double rangeX = tileSE.getX() - tileNW.getX();
          double rangeY = tileNW.getY() - tileSE.getY();
          double minLat1 = tileSE.getY() + (rangeY * (extent - maxY)) / extent;
          double maxLat1 = tileSE.getY() + (rangeY * (extent - minY)) / extent;
          double minLng1 = tileNW.getX() + rangeX * minX / extent;
          double maxLng1 = tileNW.getX() + rangeX * maxX / extent;

          minLat = Double.isNaN(minLat) ? minLat1 : Math.min(minLat, minLat1);
          minLng = Double.isNaN(minLng) ? minLng1 : Math.min(minLng, minLng1);
          maxLat = Double.isNaN(maxLat) ? maxLat1 : Math.max(maxLat, maxLat1);
          maxLng = Double.isNaN(maxLng) ? maxLng1 : Math.max(maxLng, maxLng1);
        }

        generated = date;
      }
    }

    // lenient extraction
    private static int extractInt(Map<String, Object> meta, String key, int defaultValue) {
      try {
        return Integer.valueOf(meta.get(key).toString());
      } catch(Exception e) {
        return defaultValue;
      }
    }

    // collects the year range from the metadata
    private static Int2D extractYear(Map<String, Object> meta) {
      int minYear = Integer.MAX_VALUE;
      int maxYear = Integer.MIN_VALUE;
      for (String k : meta.keySet()) {
        try {
          int year = Integer.parseInt(k); // integer keys are assumed years in the first implementation
          if (year != 0) { // the null year key
            minYear = Math.min(year, minYear);
            maxYear = Math.max(year, maxYear);
          }

        } catch(NumberFormatException e1) {
          // expected
        }
      }
      if (minYear<Integer.MAX_VALUE && maxYear > Integer.MIN_VALUE) {
        return new Int2D(minYear, maxYear);
      } else {
        return null;
      }
    }

    /**
     * Calculate the longitudinal range, allowing for crossing of the antimeridian.
     * @param values    set of values, without duplicates
     * @param maxValue  wrap value
     */
    @VisibleForTesting
    protected static int[] centredSpread(int[] values, int maxValue) {
      if (values == null || values.length == 0) {
        return null;
      }

      // Sort the values
      Arrays.sort(values);

      // Starting at this value…
      int left = 0;
      int right = 0;

      log.trace("Spread values {}", values);

      do {
        // …measure the distance to the next value to the left of the current range,
        // and the next value to the right
        double leftDist, rightDist;

        if (right+1 == values.length) {
          rightDist = (maxValue+values[0]) - values[right];
        } else {
          rightDist = values[right+1] - values[right];
        }

        if (left == 0) {
          leftDist = values[left] - (values[values.length-1]-maxValue);
        } else {
          leftDist = values[left] - values[left-1];
        }

        log.trace("Spread: left p[{}]={}, right p[{}]={}.  leftDist {}, rightDist {}: Expand {}", left, values[left], right, values[right], leftDist, rightDist, rightDist > leftDist ? "left" : "right");

        // Extend the range to envelop the nearest point to the current range
        // Favour expanding to the right (so only > not ≥ here) so a whole-world tile ends at -180–180°.
        if (rightDist > leftDist) {
          left = (left - 1 + values.length) % values.length;
        } else {
          right = (right + 1) % values.length;
        }
        log.trace("Spread: left p[{}]={}, right p[{}]={}.", left, values[left], right, values[right]);

        // Finish when we are a step away from closing the loop
      } while ((right+1)%values.length != left);

      return new int[]{values[left], values[right]};
    }
  }
}
