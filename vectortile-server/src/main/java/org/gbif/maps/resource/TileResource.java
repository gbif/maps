package org.gbif.maps.resource;

import org.gbif.maps.common.filter.TileFilters;
import org.gbif.maps.common.projection.Double2D;
import org.gbif.maps.common.projection.Mercator;
import org.gbif.maps.common.projection.TileProjection;
import org.gbif.maps.common.projection.Tiles;
import org.gbif.maps.io.PointFeature;

import java.awt.geom.AffineTransform;
import java.awt.geom.Point2D;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
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
import com.google.common.base.Stopwatch;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.vividsolutions.jts.geom.Coordinate;
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
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The tile resource for the simple gbif data layers (i.e. HBase sourced, preprocessed).
 */
@Path("/")
@Singleton
public final class TileResource {

  private static final Logger LOG = LoggerFactory.getLogger(TileResource.class);
  private static final Pattern COMMA = Pattern.compile(",");
  private static final int POINT_TILE_SIZE = 4096;
  private static final int POINT_TILE_BUFFER = 25;
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  // Maps the http parameter for the type to the HBase row key prefix for that map.
  // This aligns with the Spark processing that populates HBase of course, but maps the internal key to the
  // http parameter.
  private static final Map<String, String> MAP_TYPES = ImmutableMap.of(
    "taxonKey","1",
    "datasetKey","2",
    "publishingOrganizationKey", "3",
    "country", "4",
    "publishingCountry", "5"
  );
  private static final String ALL_MAP_KEY = "0:0";

  private final Connection connection;
  private final int tileSize;
  private final int bufferSize;

  public TileResource(Configuration conf, int tileSize, int bufferSize) throws IOException {
    connection = ConnectionFactory.createConnection(conf);
    this.tileSize = tileSize;
    this.bufferSize = bufferSize;
  }

  // TODO - of course
  LoadingCache<String, Optional<PointFeature.PointFeatures>> datasource = CacheBuilder
    .newBuilder()
    .maximumSize(1000)
    .expireAfterAccess(1, TimeUnit.MINUTES)
    .build(
      new CacheLoader<String, Optional<PointFeature.PointFeatures>>() {
        @Override
        public Optional<PointFeature.PointFeatures> load(String rowKey) throws Exception {
          try (Table table = connection.getTable(TableName.valueOf("tim_test"))) {
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes("wgs84"), Bytes.toBytes("features"));
            Result result = table.get(get);
            if (result != null) {
              byte[] encoded = result.getValue(Bytes.toBytes("wgs84"), Bytes.toBytes("features"));

              return encoded != null ? Optional.of(PointFeature.PointFeatures.parseFrom(encoded)) : Optional.<PointFeature.PointFeatures>absent();
            } else {
              return Optional.absent();
            }
          }
        }
      }
    );

  /**
   * Extracts the mapType:Key identifier from the request.
   * If an invalid request is provided (e.g. containing 2 types) then an IAE is thrown.
   * If no type is found, then the key for the all data map is given.
   */
  private static String mapKey(HttpServletRequest request) {
    Map<String, String[]> queryParams = request.getParameterMap();
    String mapKey = null;
    for (Map.Entry<String, String[]> param : queryParams.entrySet()) {
      if (MAP_TYPES.containsKey(param.getKey())) {
        if (mapKey != null || param.getValue().length!=1) {
          throw new IllegalArgumentException("Invalid request: Only one type of map may be requested.  "
                                             + "Hint: Perhaps you need to use ad hoc mapping?");
        } else {
          mapKey = MAP_TYPES.get(param.getKey()) + ":" + param.getValue()[0];
        }
      }
    }
    return mapKey == null ? ALL_MAP_KEY : mapKey;
  }

  /**
   * Converts the nullable encoded year into an array containing a minimum and maximum bounded range.
   * @param encodedYear Comma separated in min,max format (as per GBIF API)
   * @return An array of length 2, with the min and max year values, which may be NULL
   * @throws IllegalArgumentException if the year is unparsable
   */
  private static Integer[] toMinMaxYear(String encodedYear) {
    if (encodedYear == null) {
      return new Integer[]{null,null};
    } else if (encodedYear.contains(",")) {
      String[] years = COMMA.split(encodedYear);
      if (years.length == 2) {
        Integer min = null;
        Integer max = null;
        if (years[0].length() > 0) {
          min = Integer.parseInt(years[0]);
        }
        if (years[1].length() > 0) {
          max = Integer.parseInt(years[1]);
        }
        return new Integer[] {min, max};
      }
    } else {
      int year = Integer.parseInt(encodedYear);
      return new Integer[] {year, year};
    }
    throw new IllegalArgumentException("Year must contain a single or a comma separated minimum and maximum value.  "
                                       + "Supplied: " + encodedYear);
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
    @QueryParam("basisOfRecord") String basisOfRecord, // TODO: what if there are more than 1?
    @QueryParam("year") String year,
    @Context HttpServletResponse response,
    @Context HttpServletRequest request
    ) throws Exception {

    prepare(response); // headers (e.g. allow XSS)
    String mapKey = mapKey(request);
    LOG.info("MapKey: {}", mapKey);


    final TileProjection projection = Tiles.fromEPSG(srs, tileSize);

    // Try and load the point features first, before defaulting to tile views
    Optional<PointFeature.PointFeatures> optionalFeatures = datasource.get(mapKey);
    if (optionalFeatures.isPresent()) {
      final VectorTileEncoder encoder = new VectorTileEncoder(POINT_TILE_SIZE, POINT_TILE_BUFFER, false);
      PointFeature.PointFeatures features = optionalFeatures.get();

      Integer[] years = toMinMaxYear(year);
      TileFilters.collectInVectorTile(encoder, "occurrence", features.getFeaturesList(),
                                      projection, z, x, y, tileSize, bufferSize,
                                      years[0], years[1], null);

      return encoder.encode();
    } else {
      throw new IllegalArgumentException("TODO of course");
    }
  }

  // TODO: This sucks
  @GET
  @Path("/occurrence/density.json")
  @Timed
  @Produces(MediaType.APPLICATION_JSON)
  public TileJson allTileJson(@Context HttpServletResponse response) throws IOException {
    prepare(response);
    return TileJson.TileJsonBuilder
      .newBuilder()
      .withAttribution("GBIF")
      .withDescription("The tileset for the simple data layer")
      .withId("GBIF:simple")
      .withName("GBIF Occurrence Density (simple)")
      .withVectorLayers(new TileJson.VectorLayer[] {
        new TileJson.VectorLayer("occurrence", "The GBIF occurrence data")
      })
      .withTiles(new String[]{"http://localhost:7001/api/occurrence/density/{z}/{x}/{y}.mvt"})
      .build();
  }

  // open the tiles to the world (especially your friendly localhost developer!)
  private void prepare(HttpServletResponse response) {
    response.addHeader("Allow-Control-Allow-Methods", "GET,OPTIONS");
    response.addHeader("Access-Control-Allow-Origin", "*");
  }
}
