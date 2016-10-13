package org.gbif.maps.resource;

import org.gbif.maps.common.bin.HexBin;
import org.gbif.maps.common.projection.Double2D;
import org.gbif.maps.common.projection.TileProjection;
import org.gbif.maps.common.projection.Tiles;
import org.gbif.occurrence.search.heatmap.OccurrenceHeatmapRequest;
import org.gbif.occurrence.search.heatmap.OccurrenceHeatmapRequestProvider;
import org.gbif.occurrence.search.heatmap.OccurrenceHeatmapResponse;
import org.gbif.occurrence.search.heatmap.OccurrenceHeatmapsService;

import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Preconditions;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import no.ecc.vectortile.VectorTileEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.maps.resource.Params.BIN_MODE_HEX;
import static org.gbif.maps.resource.Params.DEFAULT_HEX_PER_TILE;
import static org.gbif.maps.resource.Params.HEX_TILE_SIZE;
import static org.gbif.maps.resource.Params.enableCORS;

/**
 * SOLR search as a vector tile service.
 */
@Path("/occurrence/adhoc")
@Singleton
public final class  SolrResource {

  private static final Logger LOG = LoggerFactory.getLogger(SolrResource.class);
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
  private static final double solrQueryBufferPercentage = 0.125;  // 1/8th tile buffer all around, similar to the HBase maps

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
    @QueryParam("bin") String bin,
    @DefaultValue(DEFAULT_HEX_PER_TILE) @QueryParam("hexPerTile") int hexPerTile,
    @Context HttpServletResponse response,
    @Context HttpServletRequest request
    ) throws Exception {
    enableCORS(response);
    Preconditions.checkArgument("EPSG:4326".equalsIgnoreCase(srs),
                                "Adhoc search maps are currently only available in EPSG:4326");
    OccurrenceHeatmapRequest heatmapRequest = OccurrenceHeatmapRequestProvider.buildOccurrenceHeatmapRequest(request);


    Preconditions.checkArgument(bin == null || BIN_MODE_HEX.equalsIgnoreCase(bin), "Unsupported bin mode");

    // Tim note: by testing in production index, we determine that 4 is a sensible performance choice
    // every 4 zoom levels the grid resolution increases
    //heatmapRequest.setZoom(z); // default behavior
    int solrLevel = ((int)(z/4))*4;
    heatmapRequest.setZoom(solrLevel);

    heatmapRequest.setGeometry(solrSearchGeom(z, x, y));
    LOG.info("SOLR request:{}", heatmapRequest.toString());

    OccurrenceHeatmapResponse solrResponse = solrService.searchHeatMap(heatmapRequest);
    VectorTileEncoder encoder = new VectorTileEncoder (tileSize, bufferSize, false);

    Double2D[] boundary = bufferedTileBoundary(z,x,y);
    final Point2D tileBoundarySW = new Point2D.Double(boundary[0].getX(), boundary[0].getY());
    final Point2D tileBoundaryNE = new Point2D.Double(boundary[1].getX(), boundary[1].getY());

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
              Double2D swTileXY = Tiles.toTileLocalXY(swGlobalXY, z, x, y, tileSize, bufferSize);
              Double2D neTileXY = Tiles.toTileLocalXY(neGlobalXY, z, x, y, tileSize, bufferSize);

              int minX = (int) swTileXY.getX();
              int maxX = (int) neTileXY.getX();
              double centerX = minX + (((double) maxX - minX) / 2);

              // tiles are indexed 0->255, but if the right of the cell (maxX) is on the tile boundary, this
              // will be detected (correctly) as the index 0 for the next tile.  Reset that.
              maxX = (minX > maxX) ? tileSize - 1 : maxX;

              int minY = (int) swTileXY.getY();
              int maxY = (int) neTileXY.getY();
              double centerY = minY + (((double) maxY - minY) / 2);
              // tiles are indexed 0->255, but if the bottom of the cell (maxY) is on the tile boundary, this
              // will be detected (correctly) as the index 0 for the next tile.  Reset that.
              maxY = (minY > maxY) ? tileSize - 1 : maxY;

              // Clip the extent to the tile.  At this point e.g. max can be 256px, but tile pixels can only be
              // addressed at 0 to 255.  If we don't clip, 256 will actually spill over into the second row / column
              // and result in strange lines.  Note the importance of the clipping, as the min values are the left, or
              // top of the cell, but the max values are the right or bottom.
              minX = clip(minX, 0, tileSize-1);
              maxX = clip(maxX, 1, tileSize-1);

              Map<String, Object> meta = new HashMap();
              meta.put("total", count);

              // for hexagon binning, we add the cell center point, otherwise the geometry
              if (BIN_MODE_HEX.equalsIgnoreCase(bin)) {
                // hack: use just the center points for each cell
                Coordinate center = new Coordinate(centerX, centerY);
                encoder.addFeature("occurrence", meta, GEOMETRY_FACTORY.createPoint(center));

              } else {
                // default behaviour with polygon squares for the cells
                Coordinate[] coords = new Coordinate[] {
                  new Coordinate(swTileXY.getX(), swTileXY.getY()),
                  new Coordinate(neTileXY.getX(), swTileXY.getY()),
                  new Coordinate(neTileXY.getX(), neTileXY.getY()),
                  new Coordinate(swTileXY.getX(), neTileXY.getY()),
                  new Coordinate(swTileXY.getX(), swTileXY.getY())
                };

                encoder.addFeature("occurrence", meta, GEOMETRY_FACTORY.createPolygon(coords));
              }

            }
          }
        }
      }
    }

    byte[] encodedTile = encoder.encode();
    if (BIN_MODE_HEX.equalsIgnoreCase(bin)) {
      HexBin binner = new HexBin(HEX_TILE_SIZE, hexPerTile);
      return binner.bin(encodedTile, z, x, y);

    } else {
      return encodedTile;
    }
  }

  private static int clip(int value, int lower, int upper) {
    return  Math.min(Math.max(value, lower), upper);
  }


  /**
   * Returns a SOLR search string for the geometry in WGS84 CRS for the tile with a buffer.
   */
  private static String solrSearchGeom(int z, long x, long y) {
    Double2D[] boundary = bufferedTileBoundary(z,x,y);
    return "[" + boundary[0].getX() + " " + boundary[0].getY() + " TO "
           + boundary[1].getX() + " " + boundary[1].getY() + "]";
  }

  private static Double2D[] bufferedTileBoundary(int z, long x, long y) {
    int tilesPerZoom = 1 << z;
    double degsPerTile = 360d/tilesPerZoom;

    // the edges of the tile
    double minLng = degsPerTile * x - 180;
    double maxLng = minLng + degsPerTile;
    double maxLat = 180 - (degsPerTile * y); // note EPSG:4326 covers only half the space vertically hence 180
    double minLat = maxLat-degsPerTile;

    // buffer the query by the percentage
    double bufferDegrees = solrQueryBufferPercentage*degsPerTile;
    Double2D sw = new Double2D(minLng - bufferDegrees, minLat - bufferDegrees);
    Double2D ne = new Double2D(maxLng + bufferDegrees, maxLat + bufferDegrees);

    // clip north and south, but wrap over dateline
    //Double2D clippedSW = new Double2D(sw.getX(), Math.max(sw.getY(),-90));
    //Double2D clippedNE = new Double2D(ne.getX(), Math.min(ne.getY(),90));

    // HACK FOR NOW: clip the longitude too, as SOLR does not let us cross dateline
    Double2D clippedSW = new Double2D(Math.max(sw.getX(), -180), Math.max(sw.getY(),-90));
    Double2D clippedNE = new Double2D(Math.min(ne.getX(), 180), Math.min(ne.getY(),90));

    return new Double2D[] {clippedSW, clippedNE};
  }
}
