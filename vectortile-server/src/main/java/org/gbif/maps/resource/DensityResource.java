package org.gbif.maps.resource;

import org.gbif.maps.common.projection.Mercator;
import org.gbif.maps.io.PointFeature;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.ForwardingCache;
import com.google.common.cache.LoadingCache;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import no.ecc.vectortile.VectorTileDecoder;
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
import vector_tile.VectorTile;

/**
 * Hacky tests for now.
 */
@Path("/density")
@Singleton
public final class DensityResource {

  private static final Logger LOG = LoggerFactory.getLogger(DensityResource.class);
  private static final int TILE_SIZE = 512;
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
  private static final Mercator MERCATOR = new Mercator(TILE_SIZE);

  private static final int BUFFER_SIZE = 25;
  private static final VectorTileDecoder decoder = new VectorTileDecoder();
  static {
    decoder.setAutoScale(false); // important to avoid auto scaling to 256 tiles
  }

  private final Connection connection;


  public DensityResource() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "c1n2.gbif.org:2181,c1n3.gbif.org:2181,c1n1.gbif.org:2181");
    conf.setInt("hbase.zookeeper.property.clientPort", 2181);
    connection = ConnectionFactory.createConnection(conf);
  }

  LoadingCache<String, Optional<VectorTileDecoder.FeatureIterable>> datasource = CacheBuilder
    .newBuilder()
    .maximumSize(1000)
    .expireAfterWrite(1, TimeUnit.MINUTES)
    .build(
      new CacheLoader<String, Optional<VectorTileDecoder.FeatureIterable>>() {
        @Override
        public Optional<VectorTileDecoder.FeatureIterable> load(String cell) throws Exception {
          try (Table table = connection.getTable(TableName.valueOf("tim_test"))) {
            Get get = new Get(Bytes.toBytes("0:0"));
            get.addColumn(Bytes.toBytes("merc_tiles"), Bytes.toBytes(cell));
            Result result = table.get(get);
            if (result != null) {
              byte[] encoded = result.getValue(Bytes.toBytes("merc_tiles"), Bytes.toBytes(cell));
              return encoded != null ? Optional.of(decoder.decode(encoded)) : Optional.<VectorTileDecoder.FeatureIterable>absent();
            } else {
              return Optional.absent();
            }
          }
        }
      }
    );

  @GET
  @Path("all/{z}/{x}/{y}.pbf")
  @Timed
  @Produces("application/x-protobuf")
  public byte[] all(
    @PathParam("z") int z, @PathParam("x") int x, @PathParam("y") int y,
    @Context HttpServletResponse response
  ) throws IOException, ExecutionException {
    prepare(response);
    LOG.info("{},{},{}", z, x, y);
    Stopwatch timer = new Stopwatch().start();

    String[] cells = new String[]{
      Joiner.on(":").join(z,x-1,y-1),  // NW
      Joiner.on(":").join(z,x,y-1),    // N
      Joiner.on(":").join(z,x+1,y-1),  // NE
      Joiner.on(":").join(z,x-1,y),    // E
      Joiner.on(":").join(z,x,y),      // target cell
      Joiner.on(":").join(z,x+1,y),    // W
      Joiner.on(":").join(z,x-1,y+1),  // SW
      Joiner.on(":").join(z,x,y+1),    // S
      Joiner.on(":").join(z,x+1,y+1)   // SE
    };

    /*
    // Proves slower than single calls and cache
    Result result = null;
    try (Table table = connection.getTable(TableName.valueOf("tim_test"))) {
      Get get = new Get(Bytes.toBytes("0:0"));
      for (String cell : cells) {
        get.addColumn(Bytes.toBytes("merc_tiles"), Bytes.toBytes(cell));
      }
      result = table.get(get);
    }
    */

    BufferedVectorTileEncoder encoder = new BufferedVectorTileEncoder(TILE_SIZE, BUFFER_SIZE, false);

    VectorTile tile = null;
    for (int i=0; i<cells.length; i++) {
    //for (int i=4; i<5; i++) {

      // depending on the tile (NE,N,NW...) determine the offset from the center tile
      int offsetX = TILE_SIZE * ((i%3)-1);
      int offsetY = TILE_SIZE * (((int)(i/3))-1);


      String cell = cells[i];
      //byte[] encoded  = result.getValue(Bytes.toBytes("merc_tiles"),Bytes.toBytes(cell));


      Optional<VectorTileDecoder.FeatureIterable> o = datasource.get(cell);
      if (o.isPresent()) {
        VectorTileDecoder.FeatureIterable iterable = o.get();
        //VectorTileDecoder.FeatureIterable iterable = decoder.decode(encoded);
        if (iterable != null) {
          //LOG.info("Merging data from tile {}", cell);

          for (VectorTileDecoder.Feature f : iterable) {
            Geometry geom = f.getGeometry();
            if (geom instanceof Point) {
              int px = (int) ((Point) geom).getX() + offsetX;
              int py = (int) ((Point) geom).getY() + offsetY;

              if (px > -BUFFER_SIZE && px < TILE_SIZE + BUFFER_SIZE
                  && py > -BUFFER_SIZE && py < TILE_SIZE + BUFFER_SIZE
                  && "OBSERVATION".equals(f.getLayerName())) {

                encoder.addFeature(f.getLayerName(),
                                   f.getAttributes(),
                                   GEOMETRY_FACTORY.createPoint(new Coordinate(px, py)));
              }
            }
          }
        }
      }
    }
    LOG.info("Accumulated in {}", timer.elapsedMillis());
    timer.reset();
    byte[] result = encoder.encode();
    LOG.info("Encoded in {}", timer.elapsedMillis());
    return result;
  }

  @GET
  @Path("all.json")
  @Timed
  @Produces(MediaType.APPLICATION_JSON)
  public TileJson allTileJson(@Context HttpServletResponse response) throws IOException {
    prepare(response);
    return TileJson.TileJsonBuilder
      .newBuilder()
      .withAttribution("GBIF")
      .withDescription("The tileset for all data")
      .withId("GBIF:all")
      .withName("GBIF All Data")
      .withVectorLayers(new TileJson.VectorLayer[] {
        new TileJson.VectorLayer("OBSERVATION", "The observation data")
      })
      .withTiles(new String[]{"http://localhost:7001/api/density/all/{z}/{x}/{y}.pbf"})
      .build();
  }

  // open the tiles to the world (especially your friendly localhost developer!)
  private void prepare(HttpServletResponse response) {
    response.addHeader("Allow-Control-Allow-Methods", "GET,OPTIONS");
    response.addHeader("Access-Control-Allow-Origin", "*");
  }
}
