package org.gbif.maps.vectortile.util;

import org.gbif.maps.utils.MercatorProjection;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import no.ecc.vectortile.VectorTileEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A quick test to test how big a vector tile is for various configurations.
 */
public class ExampleSizesOfMVT {
  private static final Logger LOG = LoggerFactory.getLogger(ExampleSizesOfMVT.class);
  private static final Pattern TAB = Pattern.compile("\t");
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  /**
   * Requires 1 argument which is a a tab delimited file in the format of lat, lng, year, count.
   * Anything else will fail ungracefully.
   */
  public static void main(String[] args) throws IOException {
    List<Record> records = readSource(args[0]);

    LOG.info("Collecting data to pixels");
    Map<XY, Map<Integer, AtomicInteger>> pixelData = Maps.newHashMap();
    int count = 0;
    for (Record r : records) {
      double pixelX = MercatorProjection.longitudeToPixelX(r.longitude, (byte) 0);
      double pixelY = MercatorProjection.latitudeToPixelY(r.latitude, (byte) 0);
      // find the pixel offsets local to the top left of the tile
      XY tileLocalXY = new XY ((int)Math.floor(pixelX%MercatorProjection.TILE_SIZE),
        (int)Math.floor(pixelY%MercatorProjection.TILE_SIZE));

      Map<Integer, AtomicInteger> yearAtPixel = pixelData.get(tileLocalXY);
      if (yearAtPixel == null) {
        yearAtPixel = Maps.newHashMap();
        pixelData.put(tileLocalXY, yearAtPixel);
      }

      //int year = r.year/100;
      int year = r.year;

      if (yearAtPixel.containsKey(year)) {
        yearAtPixel.get(year).addAndGet(r.count);
      } else {
        yearAtPixel.put(year, new AtomicInteger(r.count));
      }
      if (++count % 1000000 == 0) {
        LOG.info("Collected {} lines", count);
      }

    }
    LOG.info("Collected {} pixels of data", pixelData.size());
    records = null;
    System.gc();

    VectorTileEncoder encoder = new VectorTileEncoder(MercatorProjection.TILE_SIZE, 8, false);
    for (Map.Entry<XY, Map<Integer, AtomicInteger>> e : pixelData.entrySet()) {
      Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(e.getKey().x, e.getKey().y));
      Map<String, Integer> years = Maps.newHashMap();
      for (Map.Entry<Integer, AtomicInteger> e2 : e.getValue().entrySet()) {
        years.put(String.valueOf(e2.getKey()), e2.getValue().intValue());
      }
      encoder.addFeature("pointsWithYears", years, point);
    }

    byte[] mvt = encoder.encode();
    LOG.info("MVT encoded to {} Mb", (((double)mvt.length) / (1024*1024)));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ByteStreams.copy(new ByteArrayInputStream(mvt), new GZIPOutputStream(baos));
    LOG.info("MVT compressed to {} Mb", ((double)baos.toByteArray().length) / (1024*1024));

    Files.write(mvt, new File("/Users/tim/Dropbox/Public/transient/world_" + MercatorProjection.TILE_SIZE + "_1M.mvt"));
  }


  private static List<Record> readSource(String arg) throws IOException {
    LOG.info("Reading source data");
    List<Record> records = Files.readLines(new File(arg), Charset.defaultCharset(), new LineProcessor<List<Record>>() {
      List<Record> records = Lists.newArrayList();

      @Override
      public boolean processLine(String s) throws IOException {
        String[] atoms = TAB.split(s);
        records.add(new Record(
          Double.parseDouble(atoms[0]),
          Double.parseDouble(atoms[1]),
          Integer.parseInt(atoms[2]),
          Integer.parseInt(atoms[3])
        ));
        return true;
      }

      @Override
      public List<Record> getResult() {
        return records;
      }
    });

    LOG.info("Finished reading {} from source data", records.size());
    return records;
  }

  private static class Record {
    private final double latitude;
    private final double longitude;
    private final int year;
    private final int count;

    private Record(double latitude, double longitude, int year, int count) {
      this.latitude = latitude;
      this.longitude = longitude;
      this.year = year;
      this.count = count;
    }
  }

  private static class XY {
    private final int x;
    private final int y;

    XY(int x, int y) {
      this.x = x;
      this.y = y;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      XY that = (XY) o;

      return Objects.equal(this.x, that.x) &&
             Objects.equal(this.y, that.y);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(x, y);
    }
  }
}
