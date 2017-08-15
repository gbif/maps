package org.gbif.maps.resource;

import org.gbif.maps.common.filter.Range;
import org.gbif.maps.common.projection.Double2D;
import org.gbif.maps.common.projection.Int2D;

import java.io.IOException;
import java.time.Year;
import java.util.Calendar;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Maps;
import com.vividsolutions.jts.geom.Point;
import no.ecc.vectortile.VectorTileDecoder;

/**
 * The model map capability responses intended to be used for web clients to initialise map widgets for display.
 * This encapsulates the nuisance of determining the extent.
 */
public class Capabilities {
  private static final String META_TOTAL = "total";
  private final Double minLat, minLng, maxLat, maxLng;
  private final Integer minYear, maxYear;
  private final int total;
  private final String generated;

  private Capabilities(
    Double minLat,
    Double minLng,
    Double maxLat,
    Double maxLng,
    Integer minYear,
    Integer maxYear,
    int total,
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
    return floorOrCeil(minLat, -90);
  }

  public int getMinLng() {
    return floorOrCeil(minLng, -180);
  }

  public int getMaxLat() {
    return floorOrCeil(maxLat, 90);
  }

  public int getMaxLng() {
    return floorOrCeil(maxLng, 180);
  }

  public Integer getMinYear() {
    return minYear;
  }

  public Integer getMaxYear() {
    return maxYear;
  }

  public int getTotal() {
    return total;
  }

  public String getGenerated() {
    return generated;
  }

  private static int floorOrCeil(Double d, int defaultValue) {
    if (d == null) return defaultValue;
    else if (d<0) return (int) Math.floor(d);
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
    private int total;
    private String generated;

    static {
      DECODER.setAutoScale(false); // important to avoid auto scaling to 256 tiles
    }

    static CapabilitiesBuilder newBuilder() {
      return new CapabilitiesBuilder();
    }

    Capabilities build() {
      // defensively code for empty tiles
      return new Capabilities(
        Double.isNaN(minLat) ? null : minLat,
        Double.isNaN(minLng) ? null : minLng,
        Double.isNaN(maxLat) ? null : maxLat,
        Double.isNaN(maxLng) ? null : maxLng,
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
     * @param tileNW The lat,lng of the tiles North West
     * @param tileSE The lat,lng of the tiles Souyth East
     * @throws IOException On encoding issues only
     */
    void collect(byte[] tile, Double2D tileNW, Double2D tileSE, String date) throws IOException {
      if (tile != null) { // defensive coding

        double minX = Double.MAX_VALUE,  minY = Double.MAX_VALUE;
        double maxX = Double.MIN_VALUE, maxY = Double.MIN_VALUE;
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
  }
}
