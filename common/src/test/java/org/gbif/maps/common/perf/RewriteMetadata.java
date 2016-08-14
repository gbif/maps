package org.gbif.maps.common.perf;

import org.gbif.maps.io.TileFeature;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.Point;
import no.ecc.vectortile.VectorTileDecoder;
import no.ecc.vectortile.VectorTileEncoder;

/**
 * Try and store years as encoded strings, instead of KVPs.
 */
public class RewriteMetadata {

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
