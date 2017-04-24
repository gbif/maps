package org.gbif.maps.resource;

import org.gbif.maps.common.bin.HexBin;
import org.gbif.maps.common.filter.PointFeatureFilters;
import org.gbif.maps.common.filter.Range;
import org.gbif.maps.common.filter.VectorTileFilters;
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

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.vividsolutions.jts.geom.GeometryFactory;
import no.ecc.vectortile.VectorTileDecoder;
import no.ecc.vectortile.VectorTileEncoder;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.maps.resource.Params.enableCORS;
import static org.gbif.maps.resource.Params.mapKey;
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

  // we always use high resolution tiles for the point data which are small by definition
  private static final int POINT_TILE_SIZE = 4096;
  private static final int POINT_TILE_BUFFER = POINT_TILE_SIZE / 4;
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
   * @param saltModulus The salt modulus to use
   * @throws IOException If HBase cannot be reached
   */
  public TileResource(Configuration conf, String hbaseTableName, int tileSize, int bufferSize, int saltModulus)
    throws IOException {
    this.hbaseMaps = new HBaseMaps(conf, hbaseTableName, saltModulus);
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
    @Context HttpServletResponse response,
    @Context HttpServletRequest request
    ) throws Exception {

    enableCORS(response);
    String mapKey = mapKey(request);
    byte[] tile = getTile(z,x,y,mapKey,srs,basisOfRecord,year,verbose,bin,hexPerTile);

    // TODO: Set either a date or a hash as the ETag, to aid caching.
    //response.setHeader("ETag", String.format("W/\"%d\"", tile.length));
    return tile;
  }


  byte[] getTile(
    int z,
    long x,
    long y,
    String mapKey,
    String srs,
    List<String> basisOfRecord,
    String year,
    boolean verbose,
    String bin,
    int hexPerTile
  ) throws Exception {
    Preconditions.checkArgument(bin == null || BIN_MODE_HEX.equalsIgnoreCase(bin), "Unsupported bin mode");
    LOG.info("MapKey: {}", mapKey);

    Range years = toMinMaxYear(year);
    Set<String> bors = basisOfRecord == null || basisOfRecord.isEmpty() ? null : Sets.newHashSet(basisOfRecord);

    byte[] vectorTile = filteredVectorTile(z, x, y, mapKey, srs, bors, years, verbose);

    // depending on the query, direct the request
    if (bin == null) {
      return vectorTile;

    } else if (BIN_MODE_HEX.equalsIgnoreCase(bin)) {
      HexBin binner = new HexBin(HEX_TILE_SIZE, hexPerTile);
      try {
        return binner.bin(vectorTile, z, x, y);
      } catch (IllegalArgumentException e) {
        // happens on empty tiles
        return vectorTile;
      }

    } else {
      throw new IllegalArgumentException("Unsupported bin mode"); // cannot happen due to conditional check above
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
  private byte[] filteredVectorTile(int z, long x, long y, String mapKey, String srs,
                                    @Nullable Set<String> basisOfRecords, @NotNull  Range years, boolean verbose)
    throws IOException {

    // depending on the query, direct the request attempting to load the point features first, before defaulting
    // to tile views
    Optional<PointFeature.PointFeatures> optionalFeatures = hbaseMaps.getPoints(mapKey);
    if (optionalFeatures.isPresent()) {
      TileProjection projection = Tiles.fromEPSG(srs, POINT_TILE_SIZE);
      PointFeature.PointFeatures features = optionalFeatures.get();
      LOG.info("Found {} features", features.getFeaturesCount());
      VectorTileEncoder encoder = new VectorTileEncoder(POINT_TILE_SIZE, POINT_TILE_BUFFER, false);

      PointFeatureFilters.collectInVectorTile(encoder, LAYER_OCCURRENCE, features.getFeaturesList(),
                                                projection, TileSchema.fromSRS(srs), z, x, y, POINT_TILE_SIZE, POINT_TILE_BUFFER,
                                                years, basisOfRecords);

      return encoder.encode();
    } else {
      VectorTileEncoder encoder = new VectorTileEncoder(tileSize, bufferSize, false);

      Optional<byte[]> encoded = hbaseMaps.getTile(mapKey, srs, z, x, y);
      if (encoded.isPresent()) {
        LOG.info("Found tile with encoded length of: " + encoded.get().length);

        VectorTileFilters.collectInVectorTile(encoder, LAYER_OCCURRENCE, encoded.get(),
                                              years, basisOfRecords, verbose);
      }
      return encoder.encode();
    }
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
