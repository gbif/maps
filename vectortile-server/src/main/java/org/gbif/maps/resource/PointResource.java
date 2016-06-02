package org.gbif.maps.resource;

import org.gbif.maps.common.projection.Mercator;
import org.gbif.maps.io.PointFeature;

import java.awt.geom.AffineTransform;
import java.awt.geom.Point2D;
import java.io.IOException;
import java.util.HashMap;
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
import com.google.common.cache.LoadingCache;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import no.ecc.vectortile.VectorTileDecoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vector_tile.VectorTile;

/**
 * Hacky tests for now.
 */
@Path("/point")
@Singleton
public final class PointResource {

  private static final Logger LOG = LoggerFactory.getLogger(PointResource.class);
  private static final int TILE_SIZE = 512;
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
  private static final Mercator MERCATOR = new Mercator(TILE_SIZE);

  private static final int BUFFER_SIZE = 25;

  private final Connection connection;

  public PointResource() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "c1n2.gbif.org:2181,c1n3.gbif.org:2181,c1n1.gbif.org:2181");
    conf.setInt("hbase.zookeeper.property.clientPort", 2181);
    connection = ConnectionFactory.createConnection(conf);
  }

  LoadingCache<String, Optional<PointFeature.PointFeatures>> datasource = CacheBuilder
    .newBuilder()
    .maximumSize(1000)
    .expireAfterWrite(1, TimeUnit.MINUTES)
    .build(
      new CacheLoader<String, Optional<PointFeature.PointFeatures>>() {
        @Override
        public Optional<PointFeature.PointFeatures> load(String cell) throws Exception {
          try (Table table = connection.getTable(TableName.valueOf("tim_test"))) {
            Get get = new Get(Bytes.toBytes("1:2730240"));
            get.addColumn(Bytes.toBytes("wgs84"), Bytes.toBytes(cell));
            Result result = table.get(get);
            if (result != null) {
              byte[] encoded = result.getValue(Bytes.toBytes("wgs84"), Bytes.toBytes(cell));

              return encoded != null ? Optional.of(PointFeature.PointFeatures.parseFrom(encoded)) : Optional.<PointFeature.PointFeatures>absent();
            } else {
              return Optional.absent();
            }
          }
        }
      }
    );

  @GET
  @Path("all2/{z}/{x}/{y}.pbf")
  @Timed
  @Produces("application/x-protobuf")
  public byte[] all2(
    @PathParam("z") int z, @PathParam("x") int x, @PathParam("y") int y,
    @Context HttpServletResponse response
  ) throws Exception {
    prepare(response);
    LOG.info("{},{},{}", z, x, y);
    Stopwatch timer = new Stopwatch().start();

    BufferedVectorTileEncoder encoder = new BufferedVectorTileEncoder(TILE_SIZE, BUFFER_SIZE, false);

      Optional<PointFeature.PointFeatures> o = datasource.get("features");
      LOG.info("Found points: {}", o.isPresent());
      if (o.isPresent()) {

        PointFeature.PointFeatures p = o.get();

        // world pixel addressing of the tile boundary, with 0,0 at top left
        int minTilePixelX = TILE_SIZE * x;
        int minTilePixelY = TILE_SIZE * y;

        for (PointFeature.PointFeatures.Feature f : p.getFeaturesList()) {

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
            encoder.addFeature(f.getBasisOfRecord().toString(), meta, point);

            LOG.debug("Adding point at {},{}", tileLocalXY[0], tileLocalXY[1]);
          } else {
            LOG.debug("Point falls outside tile");
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
  @Path("all/{z}/{x}/{y}.pbf")
  @Timed
  @Produces("application/x-protobuf")
  public byte[] all(
    @PathParam("z") int z, @PathParam("x") int x, @PathParam("y") int y,
    @Context HttpServletResponse response
  ) throws Exception {
    prepare(response);
    LOG.info("{},{},{}", z, x, y);
    Stopwatch timer = new Stopwatch().start();

    BufferedVectorTileEncoder encoder = new BufferedVectorTileEncoder(TILE_SIZE, BUFFER_SIZE, false);

    Optional<PointFeature.PointFeatures> o = datasource.get("features");
    LOG.info("Found points: {}", o.isPresent());
    if (o.isPresent()) {

      PointFeature.PointFeatures features = o.get();

      // world pixel addressing of the tile boundary, with 0,0 at top left
      int minTilePixelX = TILE_SIZE * x;
      int minTilePixelY = TILE_SIZE * y;

      for (PointFeature.PointFeatures.Feature f : features.getFeaturesList()) {

        // TODO - big old cleanup!!!

        CoordinateReferenceSystem sourceCRS = CRS.decode("EPSG:4326");
        CoordinateReferenceSystem targetCRS = CRS.decode("EPSG:3575");
        // NOTE: Axis order for EPSG:4326 is lat,lng so invert X=latitude, Y=longitude
        // Proven with if( CRS.getAxisOrder(sourceCRS) == CRS.AxisOrder.LAT_LON) {...}
        Point orig = GEOMETRY_FACTORY.createPoint(new Coordinate(f.getLatitude(), f.getLongitude()));
        MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS, true); // lenient
        Point p = (Point) JTS.transform(orig, transform);

        double r = 40075160;  // circumference of the equator in meters

        // the world pixel range at this zoom
        double globalPixelExtent = z==0 ? TILE_SIZE : TILE_SIZE * (2<<(z-1));

        // move world coorindates into positive space addressing, so the lowest is 0,0
        AffineTransform translate= AffineTransform.getTranslateInstance(r / 2, r / 2);
        double pixelsPerMeter = globalPixelExtent / r;
        AffineTransform scale= AffineTransform.getScaleInstance(pixelsPerMeter, pixelsPerMeter);
        // Swap Y to convert world addressing to pixel addressing where 0,0 is at the top
        AffineTransform mirror_y = new AffineTransform(1, 0, 0, -1, 0, globalPixelExtent);

        // combine the transform, noting you reverse the order
        AffineTransform world2pixel = new AffineTransform(mirror_y);
        world2pixel.concatenate(scale);
        world2pixel.concatenate(translate);

        Point2D src = new Point2D.Double(p.getX(), p.getY());
        Point2D tgt = new Point2D.Double();
        world2pixel.transform(src,tgt);

        // clip to the tile
        if (tgt.getX() >= minTilePixelX && tgt.getX() <= minTilePixelX+TILE_SIZE
          && tgt.getY() >= minTilePixelY && tgt.getY() <= minTilePixelY+TILE_SIZE) {

          // modulus operation to convert to tile local pixel addressing
          Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(tgt.getX() % TILE_SIZE, tgt.getY() % TILE_SIZE));
          Map<String, Object> meta = new HashMap();
          encoder.addFeature(f.getBasisOfRecord().toString(), meta, point);
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
      .withTiles(new String[]{"http://localhost:7001/api/point/all/{z}/{x}/{y}.pbf"})
      .build();
  }

  // open the tiles to the world (especially your friendly localhost developer!)
  private void prepare(HttpServletResponse response) {
    response.addHeader("Allow-Control-Allow-Methods", "GET,OPTIONS");
    response.addHeader("Access-Control-Allow-Origin", "*");
  }
}
