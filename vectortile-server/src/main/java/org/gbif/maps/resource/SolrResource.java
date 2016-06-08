package org.gbif.maps.resource;

import org.gbif.maps.common.projection.Double2D;
import org.gbif.maps.common.projection.Mercator;
import org.gbif.maps.common.projection.TileProjection;
import org.gbif.maps.common.projection.Tiles;
import org.gbif.maps.io.PointFeature;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.inject.Singleton;
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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A quick test to try and visualise the SOLR response in a MVT.
 */
@Path("/solr")
@Singleton
public final class SolrResource {

  private static final Logger LOG = LoggerFactory.getLogger(SolrResource.class);
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();


  private final Connection connection;
  private final int tileSize;
  private final int bufferSize;
  private final HttpClient httpClient;
  private final Mercator mercator;



  public SolrResource(Configuration conf, int tileSize, int bufferSize, HttpClient httpClient) throws IOException {
    connection = ConnectionFactory.createConnection(conf);
    this.tileSize = tileSize;
    this.bufferSize = bufferSize;
    this.httpClient = httpClient;
    mercator = new Mercator(tileSize);
  }

  LoadingCache<String, Optional<PointFeature.PointFeatures>> datasource = CacheBuilder
    .newBuilder()
    .maximumSize(1000)
    .expireAfterAccess(1, TimeUnit.MINUTES)
    .build(
      new CacheLoader<String, Optional<PointFeature.PointFeatures>>() {
        @Override
        public Optional<PointFeature.PointFeatures> load(String cell) throws Exception {
          try (Table table = connection.getTable(TableName.valueOf("tim_test"))) {
            Get get = new Get(Bytes.toBytes("1:2730240"));
            //Get get = new Get(Bytes.toBytes("1:5231190")); // P. domesticus (TEST!)
            //Get get = new Get(Bytes.toBytes("1:212")); // Aves (TEST!)

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
  @Path("/{z}/{x}/{y}.pbf")
  @Timed
  @Produces("application/x-protobuf")
  public byte[] all(
    @PathParam("type") String type,
    @PathParam("key") String key,
    @PathParam("z") int z,
    @PathParam("x") long x,
    @PathParam("y") long y,
    @QueryParam("taxon_key") String taxonKey,
    @Context HttpServletResponse response
  ) throws Exception {
    prepare(response);

    // TODO: what follows is very (!) hacky tests

    String query = taxonKey == null ? "*:*" : "taxon_key:" + taxonKey;
    Stopwatch timer = new Stopwatch().start();

    HttpGet httpGet = new HttpGet("http://prodsolr05-vh.gbif.org:8983/solr/occurrencef/select?q=" + query +
                                  "&facet=true&facet.heatmap=coordinate&rows=0&facet.heatmap.distErrPct=0.1&wt=json");
    HttpResponse solrResponse = httpClient.execute(httpGet);
    HttpEntity entity = solrResponse.getEntity();
    byte[] json = EntityUtils.toByteArray(entity);


    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode rootNode = objectMapper.readTree(json);
    JsonNode facet_counts = rootNode.path("facet_counts");
    JsonNode facet_heatmaps = facet_counts.path("facet_heatmaps");
    JsonNode coordinate = facet_heatmaps.path("coordinate");
    Iterator<JsonNode> coordinates = coordinate.elements();

    // read the header
    coordinates.next();
    int gridLevel = coordinates.next().asInt();
    coordinates.next();
    int columns = coordinates.next().asInt();
    coordinates.next();
    int rows = coordinates.next().asInt();
    coordinates.next(); // minX
    coordinates.next(); // minX
    coordinates.next(); // maxX
    coordinates.next(); // maxX
    coordinates.next(); // minY
    coordinates.next(); // minY
    coordinates.next(); // maxY
    coordinates.next(); // maxY
    coordinates.next(); // counts_ints2D label

    // because the grid is in WGS84:
    double degreesPerGridX = 360d / columns;
    double degreesPerGridY = 180d / rows;

    LOG.info("rows[{}] cols[{}] degsX[{}] degsY[{}]", rows, columns, degreesPerGridX, degreesPerGridY);

    Iterator<JsonNode> dataRows = coordinates.next().iterator();

    Map<Geometry, Integer> cellsAsWGS84 = Maps.newHashMap();

    double y1 = 90;
    while(dataRows.hasNext()) {
      double x1 = -180;
      Iterator<JsonNode> dataCols = dataRows.next().iterator();
      while(dataCols.hasNext()) {
        int val = dataCols.next().asInt();

        // skip last row for bug
        if (val>0 && dataCols.hasNext()) {
          Coordinate[] coords = new Coordinate[5];
          coords[0] = new Coordinate(mercator.longitudeToTileLocalPixelX(x1, (byte) z),
                                     mercator.latitudeToTileLocalPixelY(y1, (byte) z));
          coords[1] = new Coordinate(mercator.longitudeToTileLocalPixelX(x1 + degreesPerGridX, (byte) z),
                                     mercator.latitudeToTileLocalPixelY(y1, (byte) z));
          coords[2] = new Coordinate(mercator.longitudeToTileLocalPixelX(x1 + degreesPerGridX, (byte) z),
                                     mercator.latitudeToTileLocalPixelY(y1 - degreesPerGridY, (byte) z));
          coords[3] = new Coordinate(mercator.longitudeToTileLocalPixelX(x1, (byte) z),
                                     mercator.latitudeToTileLocalPixelY(y1 - degreesPerGridY, (byte) z));
          coords[4] = coords[0];

          cellsAsWGS84.put(GEOMETRY_FACTORY.createPolygon(coords), val);
        }

        x1+=degreesPerGridX;

      }

      y1-=degreesPerGridY;
    }

    LOG.info("SOLR response in {}msecs", timer.elapsedMillis());

    BufferedVectorTileEncoder encoder = new BufferedVectorTileEncoder(tileSize, bufferSize, false);

    for (Map.Entry<Geometry, Integer> e : cellsAsWGS84.entrySet()) {
      Map<String, Object> meta = new HashMap();
      meta.put("count", e.getValue());
      encoder.addFeature("OBSERVATION", meta, e.getKey());  // TODO: OBSERVATION is obviously nonsense
    }

    timer.reset();
    byte[] result = encoder.encode();
    LOG.info("Encoded in {}", timer.elapsedMillis());
    return result;
  }

  // TODO: This sucks
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
      .withTiles(new String[]{"http://localhost:7001/api/all/{z}/{x}/{y}.pbf"})
      .build();
  }

  // open the tiles to the world (especially your friendly localhost developer!)
  private void prepare(HttpServletResponse response) {
    response.addHeader("Allow-Control-Allow-Methods", "GET,OPTIONS");
    response.addHeader("Access-Control-Allow-Origin", "*");
  }
}
