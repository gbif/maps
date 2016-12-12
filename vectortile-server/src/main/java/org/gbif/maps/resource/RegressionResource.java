package org.gbif.maps.resource;

import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.maps.common.projection.Long2D;
import org.gbif.occurrence.search.OccurrenceSearchRequestBuilder;
import org.gbif.occurrence.search.solr.OccurrenceSolrField;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;
import no.ecc.vectortile.VectorTileDecoder;
import no.ecc.vectortile.VectorTileEncoder;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.maps.resource.Params.enableCORS;
import static org.gbif.maps.resource.Params.mapKey;

/**
 * The resource for the linear regression services.
 * This is the service developed to support the species population trends application and as such is fairly tailored
 * to that need.  Should this become a more generic requirement in the future then this should be refactored.
 */
@Path("/occurrence/regression")
@Singleton
public final class RegressionResource {

  private static final Logger LOG = LoggerFactory.getLogger(RegressionResource.class);
  private static final int HEX_PER_TILE = 35;
  private static final int TILE_SIZE = 4096;
  private static final int TILE_BUFFER = TILE_SIZE / 4; // a generous buffer for hexagons
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final TileResource tiles;
  private final SolrClient solrClient;

  public RegressionResource(TileResource tiles, SolrClient solrClient) {
    this.tiles = tiles;
    this.solrClient = solrClient;
  }

  /**
   * Returns the vector tile for hte surface of information which is the result of applying the regression for the
   * given query against the higherTaxonKey.  E.g. It allows you to normalise Puma concolor against Puma or Felidae
   * depending on the higher taxonKey.
   *
   * @param z              The zoom
   * @param x              The tile x
   * @param y              The tile y
   * @param srs            The projection
   * @param higherTaxonKey The taxon key to normalize against.  This is expected to be a higher taxon of the target
   *                       species
   * @param response       The HTTP response
   * @param request        The HTTP request
   *
   * @return The vector tile showing the result of the linear regression
   */
  @GET
  @Path("/{z}/{x}/{y}.mvt")
  @Timed
  @Produces("application/x-protobuf")
  public byte[] hexagonSurface(
    @PathParam("z") int z,
    @PathParam("x") long x,
    @PathParam("y") long y,
    @DefaultValue("EPSG:3857") @QueryParam("srs") String srs,  // default as SphericalMercator
    @QueryParam("year") String year,
    @QueryParam("higherTaxonKey") String higherTaxonKey,
    @Context HttpServletResponse response,
    @Context HttpServletRequest request
  ) throws Exception {

    enableCORS(response);
    String mapKey = mapKey(request);

    byte[] speciesLayer = tiles.getTile(z, x, y, mapKey, srs, null, year, true, "hex", HEX_PER_TILE);
    mapKey = Params.MAP_TYPES.get("taxonKey") + ":" + higherTaxonKey;
    byte[] higherTaxaLayer = tiles.getTile(z, x, y, mapKey, srs, null, year, true, "hex", HEX_PER_TILE);

    // determine the global pixel origin address at the top left of the tile, used for uniquely identifying the hexagons
    Long2D originXY = new Long2D(x * TILE_SIZE, y * TILE_SIZE);

    return regression(speciesLayer, higherTaxaLayer, originXY);
  }

  /**
   * Uses the parameters to perform a search on SOLR and returns the regression.  This should use the standard
   * GBIF occurrence API parameters with the addition of higherTaxonKey.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public String adHocRegression(@Context HttpServletRequest request) throws IOException, SolrServerException {
    String higherTaxonKey = Preconditions.checkNotNull(request.getParameter("higherTaxonKey"),
                                                       "A higherTaxonKey must be provided to perform regression");

    TreeMap<String, Long> speciesCounts = yearFacetFromSolr(request);
    TreeMap<String, Long> groupCounts = yearFacetFromSolr(request, higherTaxonKey);

    // normalise all species counts against the group, building the regression the normalized data
    SimpleRegression regression = new SimpleRegression();
    groupCounts.forEach((year, baseCount) -> {
      double speciesCount = speciesCounts.containsKey(year) ? speciesCounts.get(year) : 0.0;
      if (baseCount > 0) { // defensive coding
        regression.addData(Double.valueOf(year), speciesCount / baseCount);
      }
    });

    Map<String, Object> meta = Maps.newHashMap();
    populateMetaFromRegression(meta, regression);
    meta.put("groupCounts", MAPPER.writeValueAsString(groupCounts));
    meta.put("speciesCounts", MAPPER.writeValueAsString(speciesCounts));

    return MAPPER.writeValueAsString(meta);
  }

  /**
   * Using the HTTP request containing the typical GBIF API parameters creates a SOLR faceted query for the years.
   * If a taxonKey is provided explicitly then all taxon keys in the HTTP request are ignored and replaced with these.
   */
  private TreeMap<String, Long> yearFacetFromSolr(HttpServletRequest request, String... taxonKey)
    throws IOException, SolrServerException {
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest(0, 0);
    Params.setSearchParams(searchRequest, request);
    searchRequest.getFacets().clear(); // safeguard against dangerous queries
    searchRequest.addFacets(OccurrenceSearchParameter.YEAR);

    if (taxonKey != null && taxonKey.length > 0) {
      searchRequest.getParameters().get(OccurrenceSearchParameter.TAXON_KEY).clear();
      searchRequest.getParameters().get(OccurrenceSearchParameter.TAXON_KEY).addAll(Lists.newArrayList(taxonKey));
    }

    // Default search request handler, no sort order, 1 record (required) and facet support
    OccurrenceSearchRequestBuilder builder = new OccurrenceSearchRequestBuilder("/search", null, 1, 1, true);
    SolrQuery query = builder.build(searchRequest);

    query.setFacetLimit(300); // safeguard with 3 centuries of data (this is designed for modern day analysis)
    query.setRows(0);

    LOG.debug("SOLR query: {}", query);
    QueryResponse response = solrClient.query(query);
    FacetField years = response.getFacetField(OccurrenceSolrField.YEAR.getFieldName());
    TreeMap<String, Long> yearCounts = Maps.newTreeMap();
    years.getValues().forEach(c -> yearCounts.put(c.getName(), c.getCount()));

    yearCounts.forEach((y, c) -> LOG.debug("{}:{}", y, c));

    LOG.info("SOLR response: {}", yearCounts);
    return yearCounts;
  }

  /**
   * What follows is hastily prepared  for the pilot implementation.
   * This should be refactored and unit tests added.
   */
  private byte[] regression(byte[] source, byte[] base, Long2D originXY) throws IOException {
    VectorTileDecoder decoder = new VectorTileDecoder();
    decoder.setAutoScale(false); // important to avoid auto scaling to 256 tiles

    // build an index of year counts per hexagon
    Map<String, Map<Integer, AtomicLong>> baseVals = Maps.newHashMap();
    Iterator<VectorTileDecoder.Feature> baseIter = decoder.decode(base).iterator();
    while (baseIter.hasNext()) {
      VectorTileDecoder.Feature f = baseIter.next();

      String id = getGeometryId((Polygon) f.getGeometry(), originXY);
      if (!baseVals.containsKey(id)) {
        baseVals.put(id, Maps.newHashMap());
      }
      Map<Integer, AtomicLong> yearVals = baseVals.get(id);
      for (Map.Entry<String, Object> e : f.getAttributes().entrySet()) {
        try {
          // this makes the assumption that there are year:count attributes, plus others which will be ignored
          Integer year = Integer.parseInt(e.getKey());
          if (yearVals.containsKey(year)) {
            yearVals.get(year).getAndAdd((Long) e.getValue());
          } else {
            yearVals.put(year, new AtomicLong((Long) e.getValue()));
          }
        } catch (Exception ex) {
          // ignore attributes not in year:count format
        }
      }
    }

    VectorTileEncoder encoder = new VectorTileEncoder(TILE_SIZE, TILE_BUFFER, false);
    Map<String, Object> meta = Maps.newHashMap();

    Iterator<VectorTileDecoder.Feature> sourceIter = decoder.decode(source).iterator();
    while (sourceIter.hasNext()) {
      VectorTileDecoder.Feature f = sourceIter.next();
      String id = getGeometryId((Polygon) f.getGeometry(), originXY);

      Map<Integer, AtomicLong> baseCounts = baseVals.get(id);

      if (baseCounts != null) {
        Map<Integer, AtomicLong> yearVals = Maps.newHashMap();
        for (Map.Entry<String, Object> e : f.getAttributes().entrySet()) {
          try {
            // this makes the assumption that there are year:count attributes, plus others which will be ignored
            Integer year = Integer.parseInt(e.getKey());
            if (yearVals.containsKey(year)) {
              yearVals.get(year).getAndAdd((Long) e.getValue());
            } else {
              yearVals.put(year, new AtomicLong((Long) e.getValue()));
            }
          } catch (Exception ex) {
            // ignore attributes not in year:count format
            LOG.debug("skipping metadata '{}'", e.getKey());
          }
        }

        // you need at least 2 points to regress
        if (yearVals.size() > 1) {

          // now normalize them
          SimpleRegression regression = new SimpleRegression();

          // use the group counts, since we wish to infer absence (0 count) for years where there are records within
          // the group but the species is not recorded for that year.
          for (int year : baseCounts.keySet()) {
            double speciesCount = yearVals.containsKey(year) ? (double) yearVals.get(year).get() : 0.0;
            double normalizedCount = speciesCount / baseCounts.get(year).get();
            regression.addData(Double.valueOf(year), normalizedCount);
            // add to the data that we've inferred a 0
            if (!yearVals.containsKey(year)) {
              yearVals.put(year, new AtomicLong(0));
            }
          }

          populateMetaFromRegression(meta, regression);
          meta.put("groupCounts", MAPPER.writeValueAsString(baseCounts));
          meta.put("speciesCounts", MAPPER.writeValueAsString(yearVals));

          meta.put("id", id);
          encoder.addFeature("regression", meta, f.getGeometry());
        }
      }
    }

    return encoder.encode();
  }

  /**
   * We use the global pixel address of the first vertex of the hexagon as the unique identifier.
   *
   * @return A globally unique identifier for the hexagon, that will be the same across tile boundaries (i.e. handling
   * buffers).
   */
  private String getGeometryId(Polygon geom, Long2D originXY) {
    Coordinate vertex = geom.getCoordinates()[0];
    String id = ((int) originXY.getX() + vertex.x) + ":" + ((int) originXY.getY() + vertex.y);
    return id;
  }

  /**
   * Sets the named values in the metadata from the result of the regression.
   */
  private void populateMetaFromRegression(Map<String, Object> meta, SimpleRegression regression) {
    meta.put("slope", regression.getSlope());
    meta.put("intercept", regression.getIntercept());
    meta.put("significance", regression.getSignificance());
    meta.put("SSE", regression.getSumSquaredErrors());
    meta.put("interceptStdErr", regression.getInterceptStdErr());
    meta.put("meanSquareError", regression.getMeanSquareError());
    meta.put("slopeStdErr", regression.getSlopeStdErr());
  }
}
