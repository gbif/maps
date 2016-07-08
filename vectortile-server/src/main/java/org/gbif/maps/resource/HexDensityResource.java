package org.gbif.maps.resource;

import org.gbif.maps.common.projection.Mercator;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Map;
import java.util.Set;
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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
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
import org.codetome.hexameter.core.api.DefaultSatelliteData;
import org.codetome.hexameter.core.api.Hexagon;
import org.codetome.hexameter.core.api.HexagonOrientation;
import org.codetome.hexameter.core.api.HexagonalGrid;
import org.codetome.hexameter.core.api.HexagonalGridBuilder;
import org.codetome.hexameter.core.api.HexagonalGridLayout;
import org.codetome.hexameter.core.api.SatelliteData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vector_tile.VectorTile;

/**
 * Very very hacky tests for now. (!)
 */
@Path("/hex")
@Singleton
public final class HexDensityResource {

  private static final Logger LOG = LoggerFactory.getLogger(HexDensityResource.class);
  private static final int TILE_SIZE = 512;
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
  private static final Mercator MERCATOR = new Mercator(TILE_SIZE);

  private static final int BUFFER_SIZE = 25;
  private static final VectorTileDecoder decoder = new VectorTileDecoder();
  static {
    decoder.setAutoScale(false); // important to avoid auto scaling to 256 tiles
  }

  private final Connection connection;

 public HexDensityResource() throws IOException {
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


    // TODO: decrease due to painting on large vector tiles.  Picks a sensible default
    final double radius = 20;
    final double hexWidth = radius*2;
    final double hexHeight = Math.sqrt(3) * radius;

    // World pixel addressing of the tile boundary, with 0,0 at top left
    int minTilePixelX = TILE_SIZE * x;
    int minTilePixelY = TILE_SIZE * y;

    // Set up the NxM grid of hexes, allowing for buffer of 2 hexagons all around.  If hexagons aligned perfectly
    // to the tile boundary a buffer of 1 would suffice.  However, a buffer of 2 allows us to move the grid to align
    // the hexagon polygons with the ones in the tile directly above and to the left.
    // The 3.0/2.5 factor is because we get 3 tiles in horizontal space of 2.5 widths due to the packing of hexagons
    int requiredWidth = (int)Math.ceil(TILE_SIZE * 3.0 / hexWidth * 2.5) + 4;
    int requiredHeight = (int)Math.ceil(TILE_SIZE / hexHeight) + 4;
    LOG.debug("Hex sizes {}x{} calculated grid {}x{}", hexWidth, hexHeight, requiredWidth, requiredHeight);

    HexagonalGrid grid = new HexagonalGridBuilder()
      .setGridWidth(requiredWidth)
      .setGridHeight(requiredHeight)
      .setGridLayout(HexagonalGridLayout.RECTANGULAR)
      .setOrientation(HexagonOrientation.FLAT_TOP)
      .setRadius(radius)
      .build();


    // Hexagons do not align at boundaries, and therefore we need to determine the offsets to ensure polygons
    // meet correctly across tiles.
    // The maximum offset is 1.5 cells horizontally and 1 cell vertically due to using flat top tiles.  This is
    // apparent when you see a picture. See this as an excellent resource
    // http://www.redblobgames.com/grids/hexagons/#basics
    final double offsetX = (x*((TILE_SIZE)%(1.5*hexWidth)))%(1.5*hexWidth);
    final double offsetY = (y*(TILE_SIZE%hexHeight))%hexHeight;

    // for each feature returned from the DB locate its hexagon and store the data on the hexagon
    Set<Hexagon> dataCells = Sets.newHashSet();


    VectorTile tile = null;
    for (int i=0; i<cells.length; i++) {
    //for (int i=4; i<5; i++) {

      // depending on the tile (NE,N,NW...) determine the offset from the center tile

      int tileOffsetX = (TILE_SIZE * x) + TILE_SIZE*((i%3)-1);
      int tileOffsetY = (TILE_SIZE * y) + TILE_SIZE*((i/3)-1);

      String cell = cells[i];

      Optional<VectorTileDecoder.FeatureIterable> o = datasource.get(cell);
      if (o.isPresent()) {
        VectorTileDecoder.FeatureIterable iterable = o.get();
        if (iterable != null) {
          for (VectorTileDecoder.Feature f : iterable) {
            Geometry geom = f.getGeometry();
            if (geom instanceof Point) {

              int px = (int) ((Point) geom).getX() + tileOffsetX;
              int py = (int) ((Point) geom).getY() + tileOffsetY;

              /*
              if (px > -BUFFER_SIZE && px < TILE_SIZE + BUFFER_SIZE
                  && py > -BUFFER_SIZE && py < TILE_SIZE + BUFFER_SIZE
                  && "OBSERVATION".equals(f.getLayerName())) {
                  */

                if ("OBSERVATION".equals(f.getLayerName())) {

                Hexagon hex = addFeatureInHex((byte) z,
                                              hexWidth,
                                              hexHeight,
                                              minTilePixelX,
                                              minTilePixelY,
                                              grid,
                                              offsetX,
                                              offsetY,
                                              px,
                                              py,
                                              f);
                if (hex != null) {
                  dataCells.add(hex);
                }


                //encoder.addFeature(f.getLayerName(),
                // f.getAttributes(),
                // GEOMETRY_FACTORY.createPoint(new Coordinate(px, py)));
              }
            }
          }
        }
      }
    }

    VectorTileEncoder encoder = new VectorTileEncoder(TILE_SIZE, BUFFER_SIZE, false);
    for (Hexagon hexagon : dataCells) {
      Coordinate[] coordinates = new Coordinate[7];
      int i=0;
      for(org.codetome.hexameter.core.api.Point point : hexagon.getPoints()) {
        coordinates[i++] = new Coordinate(point.getCoordinateX() - offsetX - (hexWidth*1.5),
                                          point.getCoordinateY() - offsetY - (2*hexHeight));
      }
      coordinates[6] = coordinates[0]; // close our polygon
      LinearRing linear = GEOMETRY_FACTORY.createLinearRing(coordinates);
      Polygon poly = new Polygon(linear, null, GEOMETRY_FACTORY);

      Map<String, Object> meta = Maps.newHashMap();

      // convert hexagon centers to global pixel space, and find the lat,lng centers
      meta.put("id",
               roundThreeDecimals(MERCATOR.pixelYToLatitude(minTilePixelY + hexagon.getCenterY() - offsetY - (2*hexHeight), (byte) z))
               + "," +
               roundThreeDecimals(MERCATOR.pixelXToLongitude(minTilePixelX + hexagon.getCenterX() - offsetX - (1.5*hexWidth), (byte) z))
      );

      if (hexagon.getSatelliteData().isPresent()
          && hexagon.getSatelliteData().get().getCustomData("count").isPresent()) {
        meta.put("count", hexagon.getSatelliteData().get().getCustomData("count").get());
        LOG.info("Count {}", meta.get("count"));

      }

      LOG.info("Coords {},{},{},{},{},{}" + coordinates[0], coordinates[1], coordinates[2], coordinates[3], coordinates[4], coordinates[5]);
      encoder.addFeature("OBSERVATION", meta, poly);
    }

    byte[] result = encoder.encode();
    return result;
  }


  /**
   * Adds a feature to the satellite data in the hexagon taking into account the offsets.
   * It should be noted that on a tile with 0 offset, the top left of the tile is actually covered by tile 1,0 (x,y)
   * and numbered on an odd-q vertical layout addressing scheme on http://www.redblobgames.com/grids/hexagons/.
   * @param z the zoom
   * @param hexWidth the width of a hexagon
   * @param hexHeight the height of a hexagon
   * @param minTilePixelX the minimum pixel X of the tile in world space
   * @param minTilePixelY the minimum pixel Y of the tile in world space
   * @param grid the hexagon grid
   * @param offsetX the offset for the hexagon to align with adjacent tiles
   * @param offsetY the offset for the hexagon to align with adjacent tiles
   *                px
   *                py
   * @param feature to inspect and add
   * @return the hexagon or null when the hexagon is not on the hex grid or if satellite data is null and it cannot be
   * created.
   */
  private Hexagon addFeatureInHex(
    @PathParam("z") byte z,
    double hexWidth,
    double hexHeight,
    int minTilePixelX,
    int minTilePixelY,
    HexagonalGrid grid,
    double offsetX,
    double offsetY,
    int pixelX,
    int pixelY,
    VectorTileDecoder.Feature f
  ) {

    // trim to features that lie on the tile or within a hexagon buffer
    if (pixelX >= minTilePixelX - (1.5*hexWidth) && pixelX < minTilePixelX + TILE_SIZE + (1.5*hexWidth) &&
        pixelY >= minTilePixelY - (2*hexHeight) && pixelY < minTilePixelY + TILE_SIZE + (2*hexHeight)
      ) {

      // find the pixel offset local to the top left of the tile
      double[] tileLocalXY = new double[] {pixelX - minTilePixelX, pixelY - minTilePixelY};

      // and the pixel when on hex grid space, compensating for the offset and 2 hex buffer
      double[] hexGridLocalXY = new double[] {tileLocalXY[0] + offsetX + (1.5*hexWidth), tileLocalXY[1] + offsetY + (2*hexHeight)};

      org.codetome.hexameter.core.backport.Optional<Hexagon>
        hex = grid.getByPixelCoordinate(hexGridLocalXY[0], hexGridLocalXY[1]);
      if (hex.isPresent()) {
        Hexagon hexagon = hex.get();

        if (!hexagon.getSatelliteData().isPresent()) {
          hexagon.setSatelliteData(new DefaultSatelliteData());
        }

        if (hexagon.getSatelliteData().isPresent()) {
          SatelliteData cellData = hexagon.getSatelliteData().get();

          // here are assume year:count in the attributes
          long total = 0;
          for (Map.Entry<String, Object> e : f.getAttributes().entrySet()) {
            //LOG.info("{} is instance of {}", e.getValue(), e.getValue().getClass());
            try {
              total += Long.parseLong(e.getValue().toString());
            } catch (Exception e1) {
              if (e.getValue() instanceof Long || Long.class.isAssignableFrom(e.getValue().getClass())) {
                long val = ((Long) e.getValue()).longValue();
                total += val;
              }
            }


          }

          if (!cellData.getCustomData("count").isPresent()) {
            cellData.addCustomData("count", total);
          } else {
            long existing = (Long)cellData.getCustomData("count").get();
            cellData.addCustomData("count", total + existing);
          }
        }
        return hexagon;
      }
    }
    return null;
  }

  /**
   * Rounds to 3 decimal places
   */
  private static double roundThreeDecimals(double d) {
    DecimalFormat twoDForm = new DecimalFormat("#.###");
    return Double.valueOf(twoDForm.format(d));
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
      .withTiles(new String[]{"http://localhost:7001/api/hex/all/{z}/{x}/{y}.pbf"})
      .build();
  }

  // open the tiles to the world (especially your friendly localhost developer!)
  private void prepare(HttpServletResponse response) {
    response.addHeader("Allow-Control-Allow-Methods", "GET,OPTIONS");
    response.addHeader("Access-Control-Allow-Origin", "*");
  }
}
