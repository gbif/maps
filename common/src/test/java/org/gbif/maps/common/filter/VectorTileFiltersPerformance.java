package org.gbif.maps.common.filter;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import no.ecc.vectortile.VectorTileEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.maps.common.projection.TileSchema.WEB_MERCATOR;

/**
 * A utitlity to help explore performance of filtering dense tiles.
 */
public class VectorTileFiltersPerformance {
  private static final Logger LOG = LoggerFactory.getLogger(VectorTileFiltersPerformance.class);
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
  private static final int TILE_SIZE = 512;

  private static final Set<String> BASIS_OF_RECORDS =
    ImmutableSet.of("observation", "specimen", "livingspecimen"); // do not need to be real

  /**
   * Provider of a very dense tile.
   */
  private static byte[] sampleTile() {
    VectorTileEncoder encoder = new VectorTileEncoder(TILE_SIZE, 0, false); // no buffer (like our HBase tiles)

    Map<String, Object> metadata = Maps.newHashMap();
    for (int year = 1980; year<2000; year++) {
      metadata.put(String.valueOf(year), 1);
    }

    for (int x=0; x<TILE_SIZE; x++) {
      for (int y=0; y<TILE_SIZE; y++) {
        for (String bor : BASIS_OF_RECORDS) {
          collect(encoder, bor, x, y, metadata);
        }
      }
    }
    return encoder.encode();
  }

  public static void main(String[] args) throws IOException {
    Stopwatch timer = new Stopwatch();
    timer.start();

    byte[] sample = sampleTile();
    LOG.info("Time to create sample {}ms", timer.elapsed(TimeUnit.MILLISECONDS));
    timer.reset().start();

    // loop indefinitely so we can profile
    while (true) {
      Range years = new Range(1900, 2100);
      VectorTileEncoder encoder = new VectorTileEncoder(TILE_SIZE, 0, false); // no buffer (like our HBase tiles)
      VectorTileFilters.collectInVectorTile(encoder, "occurrence", sample, WEB_MERCATOR, 0, 0, 0, 0, 0, TILE_SIZE, 20, years, BASIS_OF_RECORDS, false);
      LOG.info("Time to filter all years {}ms", timer.elapsed(TimeUnit.MILLISECONDS));
      timer.reset().start();

      years = new Range(1980, 1981);
      encoder = new VectorTileEncoder(TILE_SIZE, 0, false);
      VectorTileFilters.collectInVectorTile(encoder, "occurrence", sample, WEB_MERCATOR, 0, 0, 0, 0, 0, TILE_SIZE, 20, years, BASIS_OF_RECORDS, false);
      LOG.info("Time to filter 1 year {}ms", timer.elapsed(TimeUnit.MILLISECONDS));
      timer.reset().start();

      years = new Range(1980, 1981);
      Set<String> bor = ImmutableSet.of("observation");
      encoder = new VectorTileEncoder(TILE_SIZE, 0, false);
      VectorTileFilters.collectInVectorTile(encoder, "occurrence", sample, WEB_MERCATOR, 0, 0, 0, 0, 0, TILE_SIZE, 20, years, BASIS_OF_RECORDS, false);
      LOG.info("Time to filter 1 year, 1 bor {}ms", timer.elapsed(TimeUnit.MILLISECONDS));
      timer.reset().start();

      years = new Range(1900, 2100);
      bor = ImmutableSet.of("specimen");
      encoder = new VectorTileEncoder(TILE_SIZE, 0, false);
      VectorTileFilters.collectInVectorTile(encoder, "occurrence", sample, WEB_MERCATOR, 0, 0, 0, 0, 0, TILE_SIZE, 20, years, BASIS_OF_RECORDS, false);
      LOG.info("Time to filter all years, 1 bor {}ms", timer.elapsed(TimeUnit.MILLISECONDS));
      timer.reset().start();
    }
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
