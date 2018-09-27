package org.gbif.maps.resource;

import org.gbif.maps.common.bin.HexBin;
import org.gbif.maps.common.bin.SquareBin;
import org.gbif.maps.common.filter.PointFeatureFilters;
import org.gbif.maps.common.filter.Range;
import org.gbif.maps.common.filter.VectorTileFilters;
import org.gbif.maps.common.projection.Double2D;
import org.gbif.maps.common.projection.TileProjection;
import org.gbif.maps.common.projection.TileSchema;
import org.gbif.maps.common.projection.Tiles;
import org.gbif.maps.io.PointFeature;

import java.io.IOException;
import java.util.List;
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
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import no.ecc.vectortile.VectorTileDecoder;
import no.ecc.vectortile.VectorTileEncoder;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.maps.resource.Params.BIN_MODE_SQUARE;
import static org.gbif.maps.resource.Params.DEFAULT_SQUARE_SIZE;
import static org.gbif.maps.resource.Params.SQUARE_TILE_SIZE;
import static org.gbif.maps.resource.Params.enableCORS;
import static org.gbif.maps.resource.Params.mapKeys;
import static org.gbif.maps.resource.Params.toMinMaxYear;
import static org.gbif.maps.resource.Params.BIN_MODE_HEX;
import static org.gbif.maps.resource.Params.DEFAULT_HEX_PER_TILE;
import static org.gbif.maps.resource.Params.HEX_TILE_SIZE;

/**
 * The tile resource for the simple GBIF data layers (i.e. HBase sourced, preprocessed).
 */
@Path("/occurrence/density")
@Singleton
public final class TileResource {

  private static final Logger LOG = LoggerFactory.getLogger(TileResource.class);

  // VectorTile layer name for the composite layer produced when merging basis of record
  // layers together
  private static final String LAYER_OCCURRENCE = "occurrence";

  private static final VectorTileDecoder DECODER = new VectorTileDecoder();

  // extents of the WGS84 Plate Careé Zoom 0 tiles
  static final Double2D ZOOM_0_WEST_NW = new Double2D(-180, 90);
  static final Double2D ZOOM_0_WEST_SE = new Double2D(0, -90);
  static final Double2D ZOOM_0_EAST_NW = new Double2D(0, 90);
  static final Double2D ZOOM_0_EAST_SE = new Double2D(180, -90);

  static {
    DECODER.setAutoScale(false); // important to avoid auto scaling to 256 tiles
  }

  private final HBaseMaps hbaseMaps;
  private final int tileSize;
  private final int bufferSize;

  /**
   * Construct the resource
   * @param conf The application configuration
   * @param hbaseMaps The data layer to the maps
   * @param bufferSize The buffer size for preprocessed tiles
   * @throws IOException If HBase cannot be reached
   */
  public TileResource(Configuration conf, HBaseMaps hbaseMaps, int tileSize, int bufferSize)
    throws Exception {
    this.hbaseMaps = hbaseMaps;
    this.tileSize = tileSize;
    this.bufferSize = bufferSize;
  }

  @GET
  @Path("/{z}/{x}/{y}.mvt")
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
    @DefaultValue(DEFAULT_HEX_PER_TILE) @QueryParam("hexPerTile") int hexPerTile,
    @DefaultValue(DEFAULT_SQUARE_SIZE) @QueryParam("squareSize") int squareSize,
    @Context HttpServletResponse response,
    @Context HttpServletRequest request
    ) throws Exception {

    enableCORS(response);
    String[] mapKeys = mapKeys(request);
    DatedVectorTile datedVectorTile = getTile(z,x,y,mapKeys[0],mapKeys[1],srs,basisOfRecord,year,verbose,bin,hexPerTile,squareSize);

    if (datedVectorTile.date != null) {
      // A weak ETag is set, as tiles may not be byte-for-byte identical after filtering and binning.
      response.setHeader("ETag", String.format("W/\"%s\"", datedVectorTile.date));
    }
    return datedVectorTile.tile;
  }

  /**
   * Returns a capabilities response with the extent and year range built by inspecting the zoom 0 tiles of the
   * EPSG:4326 projection.
   */
  @GET
  @Path("capabilities.json")
  @Timed
  @Produces(MediaType.APPLICATION_JSON)
  public Capabilities capabilities(@Context HttpServletResponse response, @Context HttpServletRequest request)
    throws Exception {

    enableCORS(response);
    String[] mapKey = mapKeys(request);

    Capabilities.CapabilitiesBuilder builder = Capabilities.CapabilitiesBuilder.newBuilder();
    DatedVectorTile west = getTile(0,0,0,mapKey[0],mapKey[1],"EPSG:4326",null,null,true,null,0,0);
    DatedVectorTile east = getTile(0,1,0,mapKey[0],mapKey[1],"EPSG:4326",null,null,true,null,0,0);
    builder.collect(west.tile, ZOOM_0_WEST_NW, ZOOM_0_WEST_SE, west.date);
    builder.collect(east.tile, ZOOM_0_EAST_NW, ZOOM_0_EAST_SE, east.date);
    Capabilities capabilities = builder.build();
    LOG.info("Capabilities: {}", capabilities);

    if (capabilities.getGenerated() != null) {
      response.setHeader("ETag", String.format("\"%s\"", capabilities.getGenerated()));
    }

    return capabilities;
  }

  DatedVectorTile getTile(
    int z,
    long x,
    long y,
    String mapKey,
    String countryMaskKey,
    String srs,
    List<String> basisOfRecord,
    String year,
    boolean verbose,
    String bin,
    int hexPerTile,
    int squareSize
  ) throws Exception {
    Preconditions.checkArgument(bin == null || BIN_MODE_HEX.equalsIgnoreCase(bin) || BIN_MODE_SQUARE.equalsIgnoreCase(bin),
                                "Unsupported bin mode");
    LOG.info("MapKey: {} with mask {}", mapKey, countryMaskKey);

    Range years = toMinMaxYear(year);
    Set<String> bors = basisOfRecord == null || basisOfRecord.isEmpty() ? null : Sets.newHashSet(basisOfRecord);

    DatedVectorTile datedVectorTile = filteredVectorTile(z, x, y, mapKey, srs, bors, years, verbose);

    // If we have a country mask, retrieve the tile and apply the mask.
    if (countryMaskKey != null) {
      VectorTileEncoder encoder = new VectorTileEncoder(tileSize, tileSize/4 /* width of a hex? */, false);

      byte[] countryMaskVectorTile = filteredVectorTile(z, x, y, countryMaskKey, srs, bors, years, false).tile;

      VectorTileFilters.maskTileByTile(encoder, LAYER_OCCURRENCE, datedVectorTile.tile, countryMaskVectorTile);
      datedVectorTile.tile = encoder.encode();
    }

    // depending on the query, direct the request
    if (bin == null) {
      return datedVectorTile;

    } else if (BIN_MODE_HEX.equalsIgnoreCase(bin)) {
      HexBin binner = new HexBin(HEX_TILE_SIZE, hexPerTile);
      try {
        return new DatedVectorTile(binner.bin(datedVectorTile.tile, z, x, y), datedVectorTile.date);
      } catch (IllegalArgumentException e) {
        // happens on empty tiles
        return datedVectorTile;
      }

    } else if (BIN_MODE_SQUARE.equalsIgnoreCase(bin)) {
      SquareBin binner = new SquareBin(SQUARE_TILE_SIZE, squareSize);
      try {
        return new DatedVectorTile(binner.bin(datedVectorTile.tile, z, x, y), datedVectorTile.date);
      } catch (IllegalArgumentException e) {
        // happens on empty tiles
        return datedVectorTile;
      }

    } else {
      throw new IllegalArgumentException("Unsupported bin mode: " + bin); // cannot happen due to conditional check above
    }
  }

  /**
   * Retrieves the data from HBase, applies the filters and merges the result into a vector tile containing a single
   * layer named {@link TileResource#LAYER_OCCURRENCE}.
   * <p>
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
  private DatedVectorTile filteredVectorTile(int z, long x, long y, String mapKey, String srs,
                                    @Nullable Set<String> basisOfRecords, @NotNull Range years, boolean verbose)
    throws IOException {

    VectorTileEncoder encoder = new VectorTileEncoder(tileSize, bufferSize, false);

    // Attempt to get a preprepared tile first, before falling back to a point tile
    Optional<byte[]> encoded = hbaseMaps.getTile(mapKey, srs, z, x, y);
    String date;

    if (encoded.isPresent()) {
      date = hbaseMaps.getTileDate().orNull();
      LOG.info("Found tile {} {}/{}/{} for key {} with encoded length of {} and date {}", srs, z, x, y, mapKey, encoded.get().length, date);

      VectorTileFilters.collectInVectorTile(encoder, LAYER_OCCURRENCE, encoded.get(),
                                            years, basisOfRecords, verbose);
      return new DatedVectorTile(encoder.encode(), date);
    } else {
      // The tile size is chosen to match the size of preprepared tiles.
      date = hbaseMaps.getPointsDate().orNull();
      Optional<PointFeature.PointFeatures> optionalFeatures = hbaseMaps.getPoints(mapKey);
      if (optionalFeatures.isPresent()) {
        TileProjection projection = Tiles.fromEPSG(srs, tileSize);
        PointFeature.PointFeatures features = optionalFeatures.get();
        LOG.info("Found {} features for key {}, date {}", features.getFeaturesCount(), mapKey, date);

        PointFeatureFilters.collectInVectorTile(encoder, LAYER_OCCURRENCE, features.getFeaturesList(),
                                                projection, TileSchema.fromSRS(srs), z, x, y, tileSize, bufferSize,
                                                years, basisOfRecords);
      }
      return new DatedVectorTile(encoder.encode(), date); // may be empty
    }
  }
}
