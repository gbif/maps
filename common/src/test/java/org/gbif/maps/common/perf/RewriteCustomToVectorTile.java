package org.gbif.maps.common.perf;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.vividsolutions.jts.geom.GeometryFactory;
import no.ecc.vectortile.VectorTileDecoder;
import no.ecc.vectortile.VectorTileEncoder;

/**
 * Rewrite a custom tile to a vector tile.
 * A simple test to see how long it takes, as it would make filtering code far simpler.
 */
public class RewriteCustomToVectorTile {

  public static void main(String[] args) throws IOException {
    VectorTileDecoder decoder = new VectorTileDecoder();
    decoder.setAutoScale(false);

    GeometryFactory geometryFactory = new GeometryFactory();

    // all data, zoom 0 only with counts for each year separated by basis of record
    InputStream is = DecoderPerformance.class.getResourceAsStream("/all_z0_layers_verbose.mvt");

    byte[] bytes = ByteStreams.toByteArray(is);

    VectorTileEncoder encoder = new VectorTileEncoder(512,0,true);

    VectorTileDecoder.FeatureIterable features = decoder.decode(bytes);
    for (VectorTileDecoder.Feature f : features) {

      StringBuffer years = new StringBuffer();
      StringBuffer yearCounts = new StringBuffer();
      Map<String, Object> meta = Maps.newHashMap();
      for (String year : f.getAttributes().keySet()) {
        //years.append(year + ",");
        //yearCounts.append(f.getAttributes().get(year));
        meta.put(year, ((Long) f.getAttributes().get(year)).longValue());
      }

      //meta.put("years", years.toString());
      //meta.put("yearCounts", yearCounts.toString());
      encoder.addFeature(f.getLayerName(), meta, f.getGeometry());
    }

    byte[] encoded1 = encoder.encode();

    System.out.println("From " + bytes.length + " to " + encoded1.length + " with years as Strings");
  }
}
