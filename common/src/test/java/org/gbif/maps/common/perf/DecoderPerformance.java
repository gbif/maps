package org.gbif.maps.common.perf;

import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.google.common.io.ByteStreams;
import com.vividsolutions.jts.geom.GeometryFactory;
import no.ecc.vectortile.VectorTileDecoder;
import vector_tile.VectorTile;

/**
 * A utility to help explore the decoder performance of dense tiles.
 */
public class DecoderPerformance {

  public static void main(String[] args) throws Exception {
    FastVectorTileDecoder decoder = new FastVectorTileDecoder();
    decoder.setAutoScale(false);

    // all data, zoom 0 only with total counts
    //InputStream is = DecoderPerformance.class.getResourceAsStream("/all_z0.mvt");

    // all data, zoom 0 only with counts for each year
    InputStream is = DecoderPerformance.class.getResourceAsStream("/all_z0_verbose.mvt");


    // all data, zoom 0 only with counts for each year separated by basis of record
    //InputStream is = DecoderPerformance.class.getResourceAsStream("/all_z0_layers_verbose.mvt");



    byte[] bytes = ByteStreams.toByteArray(is);

    //VectorTile.Tile tile = VectorTile.Tile.parseFrom(bytes);
    //int[] i = tile.layers[0].features[0].geometry;
    //tile.layers[0].features[0].


    Stopwatch timer = Stopwatch.createStarted();
    for (int i=0; i<0; i++) {
      timer.reset().start();
      FastVectorTileDecoder.FeatureIterable features = decoder.decode(bytes);
      int count = 0;
      for (FastVectorTileDecoder.Feature f : features) {

        // do nothing, but iterate to force all decoding
        count++;
      }
      System.out.println("1 Read " + count + " in " + timer.elapsed(TimeUnit.MILLISECONDS) + "ms");
    }

    for (int i=0; i<1000; i++) {
      timer.reset().start();
      List<PointTiles.Feature> f = PointTiles.parsePoints(bytes);
      System.out.println("2 Read " + f.size() + " in " + timer.elapsed(TimeUnit.MILLISECONDS) + "ms");
    }

  }
}
