package org.gbif.maps.resource;

import org.gbif.maps.common.projection.Mercator;

import java.io.IOException;
import java.util.HashMap;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import no.ecc.vectortile.VectorTileDecoder;
import no.ecc.vectortile.VectorTileEncoder;
import vector_tile.VectorTile;

public class Delme {
  private static final int TILE_SIZE = 512;
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
  private static final Mercator MERCATOR = new Mercator(TILE_SIZE);

  public static void main(String[] args) throws IOException {
    VectorTileEncoder encoder = new VectorTileEncoder(TILE_SIZE, 0, false);
    encoder.addFeature("Test", new HashMap<String, Object>(), GEOMETRY_FACTORY.createPoint(new Coordinate(10,10)));
    byte[] data = encoder.encode();

    VectorTileDecoder decoder = new VectorTileDecoder();
    decoder.setAutoScale(false);
    VectorTileDecoder.FeatureIterable iterable = decoder.decode(data);
    for (VectorTileDecoder.Feature f : iterable) {
      Point p = (Point) f.getGeometry();
      System.out.println(p.getX());
      System.out.println(p.getY());
    }
  }
}
