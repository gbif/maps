package org.gbif.maps.resource;

import org.gbif.maps.common.projection.Mercator;
import org.gbif.maps.io.PointFeature;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Joiner;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import no.ecc.vectortile.VectorTileEncoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.vividsolutions.jts.geom.Coordinate;

/**
 * A simple resource that returns a demo tile.
 */
@Path("/")
@Singleton
public final class TileResource {

  private static final Logger LOG = LoggerFactory.getLogger(TileResource.class);
  private static final int TILE_SIZE = 4096;
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
  private static final Mercator MERCATOR = new Mercator(TILE_SIZE);


  private Object lock = new Object();
  private PointFeature.PointFeatures features;

  private final Connection connection;

  public TileResource() throws IOException {

    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "c1n2.gbif.org:2181,c1n3.gbif.org:2181,c1n1.gbif.org:2181");
    conf.setInt("hbase.zookeeper.property.clientPort", 2181);
    connection = ConnectionFactory.createConnection(conf);
  }

  @GET
  @Path("dataset/{key}/{z}/{x}/{y}.pbf")
  @Timed
  @Produces("application/x-protobuf")
  public byte[] dataset(
    @PathParam("key") String key,
    @PathParam("z") int z, @PathParam("x") int x, @PathParam("y") int y,
    @Context HttpServletResponse response
  ) throws IOException {
    prepare(response);
    return fromHBase(Joiner.on(":").join(2,key,z,x,y));
  }

  @GET
  @Path("taxon/{key}/{z}/{x}/{y}.pbf")
  @Timed
  @Produces("application/x-protobuf")
  public byte[] taxon(
    @PathParam("key") String key,
    @PathParam("z") int z, @PathParam("x") int x, @PathParam("y") int y,
    @Context HttpServletResponse response
  ) throws IOException {
    prepare(response);
    return fromHBase(Joiner.on(":").join(1,key), Joiner.on(":").join(z,x,y));
  }


  @GET
  @Path("all/{z}/{x}/{y}.pbf")
  @Timed
  @Produces("application/x-protobuf")
  public byte[] all(
    @PathParam("z") int z, @PathParam("x") int x, @PathParam("y") int y,
    @Context HttpServletResponse response
  ) throws IOException {
    prepare(response);
    return fromHBase(Joiner.on(":").join(0,0), Joiner.on(":").join(z,x,y));
  }

  @GET
  @Path("point/all/{z}/{x}/{y}.pbf")
  @Timed
  @Produces("application/x-protobuf")
  public byte[] pointall(
                      @PathParam("z") int z, @PathParam("x") int x, @PathParam("y") int y,
                      @Context HttpServletResponse response) throws IOException {
    prepare(response);
    try (Table table = connection.getTable(TableName.valueOf("tim_test"))) {

      synchronized (lock) {

        if (features == null) {
          Get get = new Get(Bytes.toBytes("0:0"));
          get.addColumn(Bytes.toBytes("test"), Bytes.toBytes("test"));
          Result result = table.get(get);
          byte[] encoded = result.getValue(Bytes.toBytes("test"),Bytes.toBytes("test"));
          features = PointFeature.PointFeatures.parseFrom(encoded);
          LOG.info("{} features encoded as {} kB in HBase", features.getFeaturesList().size(), encoded.length/1024);
        }
      }


      //byte[] encoded = result.getValue(Bytes.toBytes("test"),Bytes.toBytes("test"));
      //byte[] encoded = data;


      //if (encoded!=null) {
      if (features!=null) {

        // convert to MVT
        VectorTileEncoder encoder = new VectorTileEncoder(TILE_SIZE, 0, false);

        // world pixel addressing of the tile boundary, with 0,0 at top left
        int minTilePixelX = TILE_SIZE * x;
        int minTilePixelY = TILE_SIZE * y;


        for (PointFeature.PointFeatures.Feature f : features.getFeaturesList()) {
          // TODO - collect and group the locations

          double pixelX = MERCATOR.longitudeToPixelX(f.getLongitude(), (byte) z);
          double pixelY = MERCATOR.latitudeToPixelY(f.getLatitude(), (byte) z);

          // trim to features actually on the tile
          if (pixelX >= minTilePixelX && pixelX < minTilePixelX + TILE_SIZE &&
              pixelY >= minTilePixelY && pixelY < minTilePixelY + TILE_SIZE
            ) {
            // find the pixel offsets local to the top left of the tile
            int[] tileLocalXY = new int[] {(int)Math.floor(pixelX%TILE_SIZE),
              (int)Math.floor(pixelY%TILE_SIZE)};

            Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(tileLocalXY[0], tileLocalXY[1]));
            Map<String, Object> meta = new HashMap();
            encoder.addFeature("points", meta, point);



            LOG.debug("Adding point at {},{}", tileLocalXY[0], tileLocalXY[1]);
          } else {
            LOG.debug("Point falls outside tile");
          }
        }
        byte[] mvt = encoder.encode();
        LOG.info("{} features encoded as {} kB in MVT", features.getFeaturesList().size(), mvt.length/1024);
        return mvt;
      }
    }
    return null;
  }

  @GET
  @Path("point/taxon/{key}/{z}/{x}/{y}.pbf")
  @Timed
  @Produces("application/x-protobuf")
  public byte[] point(@PathParam("key") String key,
                      @PathParam("z") int z, @PathParam("x") int x, @PathParam("y") int y,
                      @Context HttpServletResponse response) throws IOException {
    prepare(response);
    try (Table table = connection.getTable(TableName.valueOf("tim_test"))) {

      synchronized (lock) {

        if (features == null) {
          Get get = new Get(Bytes.toBytes("1:" + key));
          get.addColumn(Bytes.toBytes("wgs84"), Bytes.toBytes("features"));
          Result result = table.get(get);
          byte[] encoded = result.getValue(Bytes.toBytes("wgs84"), Bytes.toBytes("features"));
          features = PointFeature.PointFeatures.parseFrom(encoded);
          LOG.info("{} features encoded as in MVT", features.getFeaturesList().size(), encoded.length/1024);
        }
      }


      //byte[] encoded = result.getValue(Bytes.toBytes("test"),Bytes.toBytes("test"));
      //byte[] encoded = data;


      //if (encoded!=null) {
      if (features!=null) {

        // convert to MVT
        VectorTileEncoder encoder = new VectorTileEncoder(TILE_SIZE, 0, false);

        // world pixel addressing of the tile boundary, with 0,0 at top left
        int minTilePixelX = TILE_SIZE * x;
        int minTilePixelY = TILE_SIZE * y;


        for (PointFeature.PointFeatures.Feature f : features.getFeaturesList()) {
          // TODO - collect and group the locations

          double pixelX = MERCATOR.longitudeToPixelX(f.getLongitude(), (byte) z);
          double pixelY = MERCATOR.latitudeToPixelY(f.getLatitude(), (byte) z);

          // trim to features actually on the tile
          if (pixelX >= minTilePixelX && pixelX < minTilePixelX + TILE_SIZE &&
              pixelY >= minTilePixelY && pixelY < minTilePixelY + TILE_SIZE
            ) {
            // find the pixel offsets local to the top left of the tile
            int[] tileLocalXY = new int[] {(int)Math.floor(pixelX%TILE_SIZE),
              (int)Math.floor(pixelY%TILE_SIZE)};

            Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(tileLocalXY[0], tileLocalXY[1]));
            Map<String, Object> meta = new HashMap();
            encoder.addFeature("points", meta, point);



            LOG.debug("Adding point at {},{}", tileLocalXY[0], tileLocalXY[1]);
          } else {
            LOG.debug("Point falls outside tile");
          }
        }
        byte[] mvt = encoder.encode();
        LOG.info("{} features encoded as in MVT", features.getFeaturesList().size(), mvt.length/1024);
        return mvt;
      }
    }
    return null;
  }

  // open the tiles to the world (especially your friendly localhost developer!)
  private void prepare(HttpServletResponse response) {
    response.addHeader("Allow-Control-Allow-Methods", "GET,OPTIONS");
    response.addHeader("Access-Control-Allow-Origin", "*");
  }

  private byte[] fromHBase(String key) throws IOException {
    try (Table table = connection.getTable(TableName.valueOf("tim_test"))) {
      Get get = new Get(Bytes.toBytes(key));
      get.addColumn(Bytes.toBytes("merc"), Bytes.toBytes("tile"));
      Result result = table.get(get);
      return result.getValue(Bytes.toBytes("merc"),Bytes.toBytes("tile"));
    }
  }

  private byte[] fromHBase(String key, String cell) throws IOException {
    try (Table table = connection.getTable(TableName.valueOf("tim_test"))) {
      Get get = new Get(Bytes.toBytes(key));
      get.addColumn(Bytes.toBytes("merc_tiles"), Bytes.toBytes(cell));
      Result result = table.get(get);
      return result.getValue(Bytes.toBytes("merc_tiles"),Bytes.toBytes(cell));
    }
  }
}
