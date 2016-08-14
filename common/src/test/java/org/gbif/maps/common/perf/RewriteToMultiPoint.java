package org.gbif.maps.common.perf;

import org.gbif.maps.common.projection.Int2D;
import org.gbif.maps.io.PointFeature;
import org.gbif.maps.io.TileFeature;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.Point;
import no.ecc.vectortile.VectorTileDecoder;
import no.ecc.vectortile.VectorTileEncoder;
import vector_tile.VectorTile;

/**
 * A test to try and rewrite the verbose tile to a multgeometry tile.
 * FAILED: only X-Y are encoded of course(!)
 */
public class RewriteToMultiPoint {

  public static void main(String[] args) throws IOException {
    VectorTileDecoder decoder = new VectorTileDecoder();
    decoder.setAutoScale(false);

    GeometryFactory geometryFactory = new GeometryFactory();

    // all data, zoom 0 only with counts for each year separated by basis of record
    InputStream is = DecoderPerformance.class.getResourceAsStream("/all_z0_layers_verbose.mvt");

    byte[] bytes = ByteStreams.toByteArray(is);

    Map<String, Map<String, List<Point>>> target = Maps.newHashMap();

    VectorTileDecoder.FeatureIterable features = decoder.decode(bytes);
    for (VectorTileDecoder.Feature f : features) {

      String layer = f.getLayerName();
      if (!target.containsKey(layer)) target.put(layer, Maps.newHashMap());
      Map<String, List<Point>> years = target.get(layer);

      for (String year : f.getAttributes().keySet()) {
        Long count = (Long)f.getAttributes().get(year);
        Point orig = (Point) f.getGeometry();

        // re-encode it with an XXZ where the z is the count
        Point xyz = geometryFactory.createPoint(new Coordinate(orig.getX(), orig.getY(), count));


        if (!years.containsKey(year)) years.put(year, Lists.newArrayList());
        List<Point> geoms = years.get(year);
        geoms.add(xyz);
      }
    }

    System.out.println(target.size());
    System.out.println(target.keySet());
    System.out.println(target.get("HUMAN_OBSERVATION").size());

    VectorTileEncoder encoder = new VectorTileEncoder(512, 0, false);
    for (Map.Entry<String, Map<String, List<Point>>> e : target.entrySet()) {
      String layer = e.getKey();
      for (Map.Entry<String, List<Point>> e1 : e.getValue().entrySet()) {
        Point[] array = e1.getValue().toArray(new Point[ e1.getValue().size()]);
        MultiPoint geom = geometryFactory.createMultiPoint(array);
        Map<String, Object> meta = Maps.newHashMap();
        meta.put("year", e1.getKey());
        encoder.addFeature(layer, meta, geom);
      }
    }

    byte[] encoded1 = encoder.encode();

    System.out.println("From " + bytes.length + " to " + encoded1.length + " as a multipoint VT");

    TileFeature.TileFeatures.Builder builder = TileFeature.TileFeatures.newBuilder();

    int totalPixels = 0;

    for (Map.Entry<String, Map<String, List<Point>>> e : target.entrySet()) {
      String layer = e.getKey();

      TileFeature.TileFeatures.Layer.Builder layerBuilder = TileFeature.TileFeatures.Layer.newBuilder();
      layerBuilder.setBasisOfRecord(TileFeature.TileFeatures.Layer.BasisOfRecord.valueOf(layer));

      for (Map.Entry<String, List<Point>> e1 : e.getValue().entrySet()) {

        for (Point px : e1.getValue()) {
          layerBuilder.addX((int)px.getX());
          layerBuilder.addY((int)px.getY());
          //layerBuilder.addYear(Integer.parseInt(e1.getKey()));
          //layerBuilder.addCount((int)px.getCoordinate().z);
          totalPixels++;
        }
      }
      builder.addLayers(layerBuilder.build());
    }

    TileFeature.TileFeatures tf = builder.build();
    System.out.println("Total: " + totalPixels);



    byte[] encoded2 = builder.build().toByteArray();
    System.out.println("From " + bytes.length + " to " + encoded2.length + " as a custom PBF");



    Files.write(encoded2, new File("/tmp/custom.pbf"));


    System.out.println( TileFeature.TileFeatures.parseFrom(encoded2).getLayers(0).getBasisOfRecord());


  }



}
