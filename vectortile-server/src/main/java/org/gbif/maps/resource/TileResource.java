package org.gbif.maps.resource;

import org.gbif.maps.common.bin.HexBin;
import org.gbif.maps.common.filter.PointFeatureFilters;
import org.gbif.maps.common.filter.Range;
import org.gbif.maps.common.filter.VectorTileFilters;
import org.gbif.maps.common.projection.Double2D;
import org.gbif.maps.common.projection.TileProjection;
import org.gbif.maps.common.projection.Tiles;
import org.gbif.maps.io.PointFeature;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.constraints.NotNull;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;
import no.ecc.vectortile.VectorTileDecoder;
import no.ecc.vectortile.VectorTileEncoder;
import org.apache.hadoop.conf.Configuration;
import org.codetome.hexameter.core.api.DefaultSatelliteData;
import org.codetome.hexameter.core.api.Hexagon;
import org.codetome.hexameter.core.api.HexagonOrientation;
import org.codetome.hexameter.core.api.HexagonalGrid;
import org.codetome.hexameter.core.api.HexagonalGridBuilder;
import org.codetome.hexameter.core.api.HexagonalGridLayout;
import org.codetome.hexameter.core.api.SatelliteData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.maps.resource.Params.enableCORS;
import static org.gbif.maps.resource.Params.mapKey;
import static org.gbif.maps.resource.Params.toMinMaxYear;
/**
 * The tile resource for the simple gbif data layers (i.e. HBase sourced, preprocessed).
 */
@Path("/")
@Singleton
public final class TileResource {

  private static final Logger LOG = LoggerFactory.getLogger(TileResource.class);

  // VectorTile layer name for the composite layer produced when merging basis of record
  // layers together
  private static final String LAYER_OCCURRENCE = "occurrence";

  // we always use high resolution tiles for the point data which are small by definition
  private static final int POINT_TILE_SIZE = 4096;
  private static final int POINT_TILE_BUFFER = POINT_TILE_SIZE / 4;
  private static final String BIN_MODE_HEX = "hex";
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
  private static final VectorTileDecoder DECODER = new VectorTileDecoder();
  static {
    DECODER.setAutoScale(false); // important to avoid auto scaling to 256 tiles
  }

  private final HBaseMaps hbaseMaps;
  private final int tileSize;
  private final int bufferSize;

  /**
   * Construct the resource
   * @param conf The application configuration
   * @param tileSize The tile size for the preprocessed tiles (not point tiles which are always full resolution)
   * @param bufferSize The buffer size for preprocessed tiles
   * @throws IOException If HBase cannot be reached
   */
  public TileResource(Configuration conf, String hbaseTableName, int tileSize, int bufferSize) throws IOException {
    this.hbaseMaps = new HBaseMaps(conf, hbaseTableName);
    this.tileSize = tileSize;
    this.bufferSize = bufferSize;
  }

  @GET
  @Path("/occurrence/density/{z}/{x}/{y}.mvt")
  @Timed
  @Produces("application/x-protobuf")
  public byte[] all(
    @PathParam("z") int z,
    @PathParam("x") long x,
    @PathParam("y") long y,
    @DefaultValue("EPSG:3857") @QueryParam("srs") String srs,  // default as SphericalMercator
    @QueryParam("basisOfRecord") List<String> basisOfRecord,
    @QueryParam("year") String year,
    @DefaultValue("false") @QueryParam("verbose") boolean verbose,
    @QueryParam("bin") String bin,
    @Context HttpServletResponse response,
    @Context HttpServletRequest request
    ) throws Exception {

    enableCORS(response);
    Preconditions.checkArgument(bin == null || BIN_MODE_HEX.equalsIgnoreCase(bin), "Unsupported bin mode");
    String mapKey = mapKey(request);
    LOG.debug("MapKey: {}", mapKey);

    Range years = toMinMaxYear(year);
    Set<String> bors = basisOfRecord.isEmpty() ? null : Sets.newHashSet(basisOfRecord);

    byte[] vectorTile = filteredVectorTile(z, x, y, mapKey, srs, bors, years, false);

    // depending on the query, direct the request
    if (bin == null) {
      return vectorTile;

    } else if (BIN_MODE_HEX.equalsIgnoreCase(bin)) {
      HexBin binner = new HexBin(4096, 37);
      return binner.bin(vectorTile, z, x, y);

    } else {
      throw new IllegalArgumentException("Unsupported bin mode"); // cannot happen due to conditional check above
    }
  }

  /**
   * Retrieves the data from HBase, applies the filters and merges the result into a vector tile containing a single
   * layer named {@link TileResource#LAYER_OCCURRENCE}.
   * <p/>
   * This method handles both pre-tiled and simple feature list stored data, returning a consistent format of vector
   * tile regardless of the storage format.  Please note that the tile size can vary and should be inspected before use.
   *
   * @param z The zoom level
   * @param x The tile X address for the vector tile
   * @param y The tile Y address for the vector tile
   * @param mapKey The map key being requested
   * @param srs The SRS of the requested tile
   * @param basisOfRecords To include in the filter.  An empty or null value will include all values
   * @param years The year range to filter.
   * @return A byte array representing an encoded vector tile.  The tile may be empty.
   * @throws IOException Only if the data in HBase is corrupt and cannot be decoded.  This is fatal.
   */
  private byte[] filteredVectorTile(int z, long x, long y, String mapKey, String srs,
                                    @Nullable Set<String> basisOfRecords, @NotNull  Range years, boolean verbose)
    throws IOException {

    // depending on the query, direct the request attempting to load the point features first, before defaulting
    // to tile views
    Optional<PointFeature.PointFeatures> optionalFeatures = hbaseMaps.getPoints(mapKey);
    if (optionalFeatures.isPresent()) {
      TileProjection projection = Tiles.fromEPSG(srs, POINT_TILE_SIZE);
      PointFeature.PointFeatures features = optionalFeatures.get();
      LOG.debug("Found {} features", features.getFeaturesCount());
      VectorTileEncoder encoder = new VectorTileEncoder(POINT_TILE_SIZE, POINT_TILE_BUFFER, false);
      PointFeatureFilters.collectInVectorTile(encoder, LAYER_OCCURRENCE, features.getFeaturesList(),
                                              projection, z, x, y, POINT_TILE_SIZE, POINT_TILE_BUFFER,
                                              years, basisOfRecords);
      return encoder.encode();
    } else {
      VectorTileEncoder encoder = new VectorTileEncoder(tileSize, bufferSize, false);

      Optional<byte[]> encoded = hbaseMaps.getTile(mapKey, srs, z, x, y);
      if (encoded.isPresent()) {
        LOG.debug("Found tile with encoded length of: " + encoded.get().length);

        VectorTileFilters.collectInVectorTile(encoder, LAYER_OCCURRENCE, encoded.get(),
                                              years, basisOfRecords, verbose);
      }
      return encoder.encode();
    }
  }


  private VectorTileEncoder encoderForHexBins(String mapKey,
                                              int z, long x, long y, String srs, Range years, Set<String> bors) {

    // Try and load the point features first, before defaulting to tile views
    Optional<PointFeature.PointFeatures> optionalFeatures = hbaseMaps.getPoints(mapKey);
    if (optionalFeatures.isPresent()) {


      final int hexPerTile = 25; // TODO - configurify
      final double radius = POINT_TILE_SIZE / (hexPerTile * 2.5);
      final double hexWidth = radius*2;
      final double hexHeight = Math.sqrt(3) * radius;

      // World pixel addressing of the tile boundary, with 0,0 at top left
      long minTilePixelX = POINT_TILE_SIZE * x;
      long minTilePixelY = POINT_TILE_SIZE * y;

      // Set up the NxM grid of hexes, allowing for buffer of 2 hexagons all around.  If hexagons aligned perfectly
      // to the tile boundary a buffer of 1 would suffice.  However, a buffer of 2 allows us to move the grid to align
      // the hexagon polygons with the ones in the tile directly above and to the left.
      // The 3.0/2.5 factor is because we get 3 tiles in horizontal space of 2.5 widths due to the packing of hexagons
      int requiredWidth = (int)Math.ceil(POINT_TILE_SIZE * 3.0 / hexWidth * 2.5) + 4;
      int requiredHeight = (int)Math.ceil(POINT_TILE_SIZE / hexHeight) + 4;
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
      final double offsetX = (x*((POINT_TILE_SIZE)%(1.5*hexWidth)))%(1.5*hexWidth);
      final double offsetY = (y*(POINT_TILE_SIZE%hexHeight))%hexHeight;

      // for each feature returned from the datastore locate its hexagon and store the data on the hexagon
      Set<Hexagon> dataCells = Sets.newHashSet();
      TileProjection projection = Tiles.fromEPSG(srs, POINT_TILE_SIZE);
      for (PointFeature.PointFeatures.Feature feature : optionalFeatures.get().getFeaturesList()) {

        Double2D globalPixelXY = projection.toGlobalPixelXY(feature.getLatitude(), feature.getLongitude(), z);
        Double2D tileLocalXY = Tiles.toTileLocalXY(globalPixelXY, x, y, tileSize);


        Hexagon hex = addFeatureInHex((byte) z,
                                      hexWidth,
                                      hexHeight,
                                      minTilePixelX,
                                      minTilePixelY,
                                      grid,
                                      offsetX,
                                      offsetY,
                                      tileLocalXY.getX(),
                                      tileLocalXY.getY(),
                                      feature);
        if (hex != null) {
          dataCells.add(hex);
        }
      }


      VectorTileEncoder encoder = new VectorTileEncoder(POINT_TILE_SIZE, POINT_TILE_BUFFER, false);
      for (Hexagon hexagon : dataCells) {
        Coordinate[] coordinates = new Coordinate[7];
        int i = 0;
        for (org.codetome.hexameter.core.api.Point point : hexagon.getPoints()) {
          coordinates[i++] = new Coordinate(point.getCoordinateX() - offsetX - (hexWidth * 1.5),
                                            point.getCoordinateY() - offsetY - (2 * hexHeight));
        }
        coordinates[6] = coordinates[0]; // close our polygon
        LinearRing linear = GEOMETRY_FACTORY.createLinearRing(coordinates);
        Polygon poly = new Polygon(linear, null, GEOMETRY_FACTORY);

        Map<String, Object> meta = Maps.newHashMap();

        // HACK: a test id
        meta.put("id",
                 roundThreeDecimals(hexagon.getCenterY())
                 + "," +
                 roundThreeDecimals(hexagon.getCenterX())
        );

        if (hexagon.getSatelliteData().isPresent()
            && hexagon.getSatelliteData().get().getCustomData("total").isPresent()) {
          meta.put("total", hexagon.getSatelliteData().get().getCustomData("total").get());
          LOG.info("total {}", meta.get("total"));

        }

        LOG.info("Coords {},{},{},{},{},{}" + coordinates[0],
                 coordinates[1],
                 coordinates[2],
                 coordinates[3],
                 coordinates[4],
                 coordinates[5]);
        encoder.addFeature("occurrence", meta, poly);
      }

      return encoder;

    } else {
      // do clever stuff
      throw new IllegalStateException("Unfinished work");
    }

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
    long minTilePixelX,
    long minTilePixelY,
    HexagonalGrid grid,
    double offsetX,
    double offsetY,
    double pixelX,
    double pixelY,
    PointFeature.PointFeatures.Feature f
  ) {

    // trim to features that lie on the tile or within a hexagon buffer
    if (pixelX >= minTilePixelX - (1.5*hexWidth) && pixelX < minTilePixelX + POINT_TILE_SIZE + (1.5*hexWidth) &&
        pixelY >= minTilePixelY - (2*hexHeight) && pixelY < minTilePixelY + POINT_TILE_SIZE + (2*hexHeight)
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

          // HACK!!!
          long total = 10;
          if (!cellData.getCustomData("total").isPresent()) {
            cellData.addCustomData("total", total);
          } else {
            long existing = (Long)cellData.getCustomData("total").get();
            cellData.addCustomData("total", total + existing);
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

  /*
@GET
  @Path("/occurrence/density.json")
  @Timed
  @Produces(MediaType.APPLICATION_JSON)
  public TileJson allTileJson(@Context HttpServletResponse response, @Context HttpServletRequest request) throws IOException {
    enableCORS(response);
    return TileJson.TileJsonBuilder
      .newBuilder()
      .withAttribution("GBIF")
      .withDescription("The tileset for the simple data layer")
      .withId("GBIF:simple")
      .withName("GBIF Occurrence Density (simple)")
      .withVectorLayers(new TileJson.VectorLayer[] {
        new TileJson.VectorLayer("occurrence", "The GBIF occurrence data")
      })
      .withTiles(new String[]{"http://localhost/api/occurrence/density/{z}/{x}/{y}.mvt?" + request.getQueryString()})
      .build();
  }
  */
}
