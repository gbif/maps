package org.gbif.maps.resource;

import org.gbif.maps.common.filter.PointFeatureFilters;
import org.gbif.maps.common.filter.Range;
import org.gbif.maps.common.filter.VectorTileFilters;
import org.gbif.maps.common.projection.TileProjection;
import org.gbif.maps.common.projection.Tiles;
import org.gbif.maps.io.PointFeature;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.vividsolutions.jts.geom.GeometryFactory;
import no.ecc.vectortile.VectorTileDecoder;
import no.ecc.vectortile.VectorTileEncoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.maps.resource.Params.*;
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
  private static final int POINT_TILE_BUFFER = 25;

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
    @Context HttpServletResponse response,
    @Context HttpServletRequest request
    ) throws Exception {

    enableCORS(response);
    String mapKey = mapKey(request);
    LOG.debug("MapKey: {}", mapKey);

    // Try and load the point features first, before defaulting to tile views
    Optional<PointFeature.PointFeatures> optionalFeatures = hbaseMaps.getPoints(mapKey);
    if (optionalFeatures.isPresent()) {
      TileProjection projection = Tiles.fromEPSG(srs, POINT_TILE_SIZE);

      PointFeature.PointFeatures features = optionalFeatures.get();
      LOG.info("Found {} features", features.getFeaturesCount());
      final VectorTileEncoder encoder = new VectorTileEncoder(POINT_TILE_SIZE, POINT_TILE_BUFFER, false);
      Range years = toMinMaxYear(year);
      Set<String> bors = basisOfRecord.isEmpty() ? null : Sets.newHashSet(basisOfRecord);
      PointFeatureFilters.collectInVectorTile(encoder, LAYER_OCCURRENCE, features.getFeaturesList(),
                                              projection, z, x, y, POINT_TILE_SIZE, bufferSize,
                                              years, bors);

      return encoder.encode();
    } else {
      VectorTileEncoder encoder = new VectorTileEncoder(tileSize, bufferSize, false);

      // try and load a prepared tiles from HBase, using adjacent tiles to populate the buffer
      Range years = toMinMaxYear(year);
      Set<String> bors = basisOfRecord.isEmpty() ? null : Sets.newHashSet(basisOfRecord);

      // Getting 9 tiles from HBase is hopelessly slow.  Here I am exploring what the performance would be if I
      // were to get the VectorTiles pre-bufferred in HBase (requires Spark backfill changes).
      LOG.info("NOTE: skipping adjacent tiles, which will likely lead to tile boundary issues");
      for (long y1=y; y1<=y; y1++) {
        for (long x1=x; x1<=x; x1++) {

      // TODO: This is wrong for dateline handling, and should determine if there can possibly be a tile
      //for (long y1=y-1; y1<=y+1; y1++) {
      //  for (long x1=x-1; x1<=x+1; x1++) {
          Optional<byte[]> encoded = hbaseMaps.getTile(mapKey, srs, z, x1, y1);
          //LOG.info("NOTE: skipping caching of HBase responses!");
          //Optional<byte[]> encoded = hbaseMaps.getTileNoCache(mapKey, srs, z, x1, y1);

          if (encoded.isPresent()) {
            LOG.debug("Found tile with encoded length of: " + encoded.get().length);

            VectorTileFilters.collectInVectorTile(encoder, LAYER_OCCURRENCE, encoded.get(),
                                                  z, x, y, x1, y1, tileSize, bufferSize,
                                                  years, bors, false);
          }
        }
      }
      return encoder.encode(); // could be empty(!)
    }
  }

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


}
