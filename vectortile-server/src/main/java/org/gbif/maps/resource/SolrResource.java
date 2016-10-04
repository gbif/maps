package org.gbif.maps.resource;

import org.gbif.maps.common.projection.Double2D;
import org.gbif.maps.common.projection.Mercator;
import org.gbif.maps.common.projection.TileProjection;
import org.gbif.maps.common.projection.Tiles;
import org.gbif.maps.io.PointFeature;
import org.gbif.occurrence.search.heatmap.OccurrenceHeatmapRequest;
import org.gbif.occurrence.search.heatmap.OccurrenceHeatmapRequestProvider;
import org.gbif.occurrence.search.heatmap.OccurrenceHeatmapResponse;
import org.gbif.occurrence.search.heatmap.OccurrenceHeatmapsService;

import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import no.ecc.vectortile.VectorTileEncoder;
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

  private final int tileSize;
  private final int bufferSize;
  private final TileProjection projection;
  private final OccurrenceHeatmapsService solrService;

  public SolrResource(OccurrenceHeatmapsService solrService, int tileSize, int bufferSize) throws IOException {
    this.tileSize = tileSize;
    this.bufferSize = bufferSize;
    this.solrService = solrService;
    projection = Tiles.fromEPSG("EPSG:4326", tileSize);
  }

  @GET
  @Path("/{z}/{x}/{y}.mvt")
  @Timed
  @Produces("application/x-protobuf")
  public byte[] all(
    @PathParam("type") String type,
    @PathParam("key") String key,
    @PathParam("z") int z,
    @PathParam("x") long x,
    @PathParam("y") long y,
    @DefaultValue("EPSG:4326") @QueryParam("srs") String srs,
    @Context HttpServletResponse response,
    @Context HttpServletRequest request
    ) throws Exception {
    prepare(response);
    Preconditions.checkArgument("EPSG:4326".equalsIgnoreCase(srs),
                                "Adhoc search maps are currently only available in EPSG:4326");
    OccurrenceHeatmapRequest heatmapRequest = OccurrenceHeatmapRequestProvider.buildOccurrenceHeatmapRequest(request);
    //heatmapRequest.setZoom(z);
    // Tim note: by testing in production index, we determine that 4 is a sensible performance choice
    // every 4 zoom levels the grid resolution increases
    int solrLevel = ((int)(z/4))*4;
    heatmapRequest.setZoom(solrLevel);

    heatmapRequest.setGeometry(solrSearchGeom(z, x, y));
    LOG.info("SOLR request:{}", heatmapRequest.toString());

    OccurrenceHeatmapResponse solrResponse = solrService.searchHeatMap(heatmapRequest);
    VectorTileEncoder encoder = new VectorTileEncoder (tileSize, bufferSize, false);

    final Rectangle2D.Double tileBoundary = tileBoundaryWGS84(z, x, y); // TODO: merge with solrSearchGeom
    final Point2D tileBoundarySW = new Point2D.Double(tileBoundary.getMinX(), tileBoundary.getMinY());
    final Point2D tileBoundaryNE = new Point2D.Double(tileBoundary.getMaxX(), tileBoundary.getMaxY());

   // iterate the data structure from SOLR painting cells
    final List<List<Integer>> countsInts = solrResponse.getCountsInts2D();
    for (int row = 0; countsInts!=null && row < countsInts.size(); row++) {
      if (countsInts.get(row) != null) {
        for (int column = 0; column < countsInts.get(row).size(); column++) {
          Integer count = countsInts.get(row).get(column);
          if (count != null && count > 0) {

            final Rectangle2D.Double cell = new Rectangle2D.Double(solrResponse.getMinLng(column),
                                                                   solrResponse.getMinLat(row),
                                                                   solrResponse.getMaxLng(column)
                                                                   - solrResponse.getMinLng(column),
                                                                   solrResponse.getMaxLat(row) - solrResponse.getMinLat(
                                                                     row));

            // get the extent of the cell
            final Point2D cellSW = new Point2D.Double(cell.getMinX(), cell.getMinY());
            final Point2D cellNE = new Point2D.Double(cell.getMaxX(), cell.getMaxY());

            // only paint if the cell falls on the tile (noting again higher Y means further south).
            if (cellNE.getX() > tileBoundarySW.getX() && cellSW.getX() < tileBoundaryNE.getX()
                && cellNE.getY() > tileBoundarySW.getY() && cellSW.getY() < tileBoundaryNE.getY()) {

              // clip normalized pixel locations to the edges of the cell
              double minXAsNorm = Math.max(cellSW.getX(), tileBoundarySW.getX());
              double maxXAsNorm = Math.min(cellNE.getX(), tileBoundaryNE.getX());
              double minYAsNorm = Math.max(cellSW.getY(), tileBoundarySW.getY());
              double maxYAsNorm = Math.min(cellNE.getY(), tileBoundaryNE.getY());

              // convert the lat,lng into pixel coordinates
              Double2D swGlobalXY = projection.toGlobalPixelXY(maxYAsNorm, minXAsNorm, z);
              Double2D neGlobalXY = projection.toGlobalPixelXY(minYAsNorm, maxXAsNorm, z);
              Double2D swTileXY = Tiles.toTileLocalXY(swGlobalXY, x, y, tileSize);
              Double2D neTileXY = Tiles.toTileLocalXY(neGlobalXY, x, y, tileSize);

              int minX = (int) swTileXY.getX();
              int maxX = (int) neTileXY.getX();
              // tiles are indexed 0->255, but if the right of the cell (maxX) is on the tile boundary, this
              // will be detected (correctly) as the index 0 for the next tile.  Reset that.
              maxX = (minX > maxX) ? tileSize - 1 : maxX;

              int minY = (int) swTileXY.getY();
              int maxY = (int) neTileXY.getY();
              // tiles are indexed 0->255, but if the bottom of the cell (maxY) is on the tile boundary, this
              // will be detected (correctly) as the index 0 for the next tile.  Reset that.
              maxY = (minY > maxY) ? tileSize - 1 : maxY;

              // Clip the extent to the tile.  At this point e.g. max can be 256px, but tile pixels can only be
              // addressed at 0 to 255.  If we don't clip, 256 will actually spill over into the second row / column
              // and result in strange lines.  Note the importance of the clipping, as the min values are the left, or
              // top of the cell, but the max values are the right or bottom.
              minX = clip(minX, 0, tileSize-1);
              maxX = clip(maxX, 1, tileSize-1);

              Coordinate[] coords = new Coordinate[] {
                new Coordinate(swTileXY.getX(), swTileXY.getY()),
                new Coordinate(neTileXY.getX(), swTileXY.getY()),
                new Coordinate(neTileXY.getX(), neTileXY.getY()),
                new Coordinate(swTileXY.getX(), neTileXY.getY()),
                new Coordinate(swTileXY.getX(), swTileXY.getY())
              };

              Map<String, Object> meta = new HashMap();
              meta.put("total", count);
              encoder.addFeature("occurrence", meta, GEOMETRY_FACTORY.createPolygon(coords));
            }
          }
        }
      }
    }
    return encoder.encode();
  }

  private static int clip(int value, int lower, int upper) {
    return  Math.min(Math.max(value, lower), upper);
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
        new TileJson.VectorLayer("occurrence", "The observation data")
      })
      .withTiles(new String[]{"http://tiletest.gbif.org:9001/api/solr/{z}/{x}/{y}.mvt"})
      .build();
  }

  // open the tiles to the world (especially your friendly localhost developer!)
  private void prepare(HttpServletResponse response) {
    response.addHeader("Allow-Control-Allow-Methods", "GET,OPTIONS");
    response.addHeader("Access-Control-Allow-Origin", "*");
  }

  /**
   * Returns a SOLR search string for the geometry in WGS84 CRS for the tile.
   */
  private static String solrSearchGeom(int z, long x, long y) {
    int tilesPerZoom = 1 << z;
    double degsPerTile = 360d/tilesPerZoom;
    double minLng = degsPerTile * x - 180;
    double maxLat = 180 - (degsPerTile * y); // note EPSG:4326 covers only half the space vertically hence 180

    // clip to the world extent
    maxLat = Math.min(maxLat,90);
    double minLat = Math.max(maxLat-degsPerTile,-90);
    return "[" + minLng + " " + minLat + " TO "
           + (minLng+degsPerTile) + " " + maxLat + "]";
  }

  /**
   * Returns a SOLR search string for the geometry in WGS84 CRS for the tile.
   */
  private static Rectangle2D.Double tileBoundaryWGS84(int z, long x, long y) {
    int tilesPerZoom = 1 << z;
    double degsPerTile = 360d/tilesPerZoom;
    double minLng = degsPerTile * x - 180;
    double maxLat = 180 - (degsPerTile * y); // note EPSG:4326 covers only half the space vertically hence 180

    // clip to the world extent
    maxLat = Math.min(maxLat,90);
    double minLat = Math.max(maxLat-degsPerTile,-90);
    return new Rectangle2D.Double(minLng, minLat, degsPerTile, maxLat - minLat);
  }
}
