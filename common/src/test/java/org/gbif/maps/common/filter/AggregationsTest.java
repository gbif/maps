package org.gbif.maps.common.filter;

import com.google.common.io.Resources;
import no.ecc.vectortile.VectorTileDecoder;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.ToLongFunction;

import static org.junit.Assert.assertEquals;

/**
 * Tests relating to aggregation of counts from 4 tiles to the parent tile.
 * See https://github.com/gbif/maps/issues/48.
 */
public class AggregationsTest {

  private static final int TILE_SIZE = 512; // GBIF tile size, and size of tiles in src/test/resources/...

  // the parent tile should represent the aggregation of the NE,NW,SE,SW quad of tiles
  List<VectorTileDecoder.Feature> parent, NE, NW, SE, SW;

  /** Reads the tiles from the protobuf into objects */
  @Before
  public void loadTiles() throws IOException {
    VectorTileDecoder decoder = new VectorTileDecoder();
    decoder.setAutoScale(false); // don't reproject to 256 pixel sizes

    parent = decoder.decode(Resources.toByteArray(Resources.getResource("tiles/3_8_2.mvt"))).asList();
    NE = decoder.decode(Resources.toByteArray(Resources.getResource("tiles/4_16_4.mvt"))).asList();
    NW = decoder.decode(Resources.toByteArray(Resources.getResource("tiles/4_17_4.mvt"))).asList();
    SE = decoder.decode(Resources.toByteArray(Resources.getResource("tiles/4_16_5.mvt"))).asList();
    SW = decoder.decode(Resources.toByteArray(Resources.getResource("tiles/4_17_5.mvt"))).asList();
  }

  @Test
  public void testTotalAggregation() {
    long p = parent.stream().mapToLong(new TotalFromTile()).sum();
    long ne = NE.stream().mapToLong(new TotalFromTile()).sum();
    long nw = NW.stream().mapToLong(new TotalFromTile()).sum();
    long se = SE.stream().mapToLong(new TotalFromTile()).sum();
    long sw = SW.stream().mapToLong(new TotalFromTile()).sum();

    assertEquals("Parent tile total should match sum of children", ne+nw+se+sw, p);
  }

  @Test
  public void testTotalVerbose() {
    Map<String, Long> years = new HashMap<>();
    parent.stream().forEach(f -> {
      f.getAttributes().entrySet().stream().forEach(a -> {
        long current = years.getOrDefault(a.getKey(), 0l);
        years.put (a.getKey(), current+ (Long)a.getValue());
      });
    });

    long total = years.get("total");
    long yearSum = years.entrySet().stream().filter(a -> !a.getKey().equals("total")).mapToLong(a -> a.getValue()).sum();
    assertEquals("Sum of year values should equal total", yearSum, total);
  }

  /**  Extract the total, excluding the bounds */
  public static class TotalFromTile implements ToLongFunction<VectorTileDecoder.Feature> {

    @Override
    public long applyAsLong(VectorTileDecoder.Feature a) {
      double x = a.getGeometry().getCoordinate().x;
      double y = a.getGeometry().getCoordinate().y;
      if (x<0 || y<0 || x>=TILE_SIZE || y>=TILE_SIZE) {
        return 0;
      } else {
        return (Long) a.getAttributes().get("total");
      }
    }
  }
}
