package org.gbif.maps.resource;

import org.gbif.maps.common.bin.HexBin;
import org.gbif.maps.common.bin.SquareBin;
import org.gbif.maps.common.projection.Double2D;
import org.gbif.maps.common.projection.Long2D;
import org.gbif.maps.common.projection.TileProjection;
import org.gbif.maps.common.projection.TileSchema;
import org.gbif.maps.common.projection.Tiles;
import org.gbif.occurrence.search.heatmap.OccurrenceHeatmapRequest;
import org.gbif.occurrence.search.heatmap.OccurrenceHeatmapRequestProvider;
import org.gbif.occurrence.search.heatmap.OccurrenceHeatmapResponse;
import org.gbif.occurrence.search.heatmap.OccurrenceHeatmapsService;

import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import no.ecc.vectortile.VectorTileEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.maps.resource.Params.BIN_MODE_HEX;
import static org.gbif.maps.resource.Params.BIN_MODE_SQUARE;
import static org.gbif.maps.resource.Params.DEFAULT_HEX_PER_TILE;
import static org.gbif.maps.resource.Params.DEFAULT_SQUARE_SIZE;
import static org.gbif.maps.resource.Params.HEX_TILE_SIZE;
import static org.gbif.maps.resource.Params.SQUARE_TILE_SIZE;
import static org.gbif.maps.resource.Params.enableCORS;

/**
 * SOLR search as a vector tile service.
 * Note to developers: This class could benefit from some significant refactoring and cleanup.
 */
@Path("/occurrence/adhoc")
@Singleton
public final class SolrResource {

  private static final Logger LOG = LoggerFactory.getLogger(SolrResource.class);
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  @VisibleForTesting
  static final double SOLR_QUERY_BUFFER_PERCENTAGE = 0.125;  // 1/8th tile buffer all around, similar to the HBase maps
  private static final String EPSG_4326 = "EPSG:4326";

  private final int tileSize;
  private final int bufferSize;
  private final TileProjection projection;
  private final OccurrenceHeatmapsService solrService;

  //Experimental: keeps a cache of all the calculated geometries from z,x and y.
  private final LoadingCache<ColRow,Bbox2D> colRowToBbox = CacheBuilder.newBuilder().build(CacheLoader.from(colRow -> {


    // get the extent of the cell
    final Point2D cellSW = new Point2D.Double(colRow.cell.getMinX(), colRow.cell.getMinY());
    final Point2D cellNE = new Point2D.Double(colRow.cell.getMaxX(), colRow.cell.getMaxY());

    // convert the lat,lng into pixel coordinates
    Long2D topLeftTile = getTopLeftTile(cellSW, cellNE, colRow.z, colRow.x, colRow.y);
    Long2D bottomRightTile = getBottomRightTile(cellSW, cellNE, colRow.z, colRow.x, colRow.y);
    return Bbox2D.of(topLeftTile, bottomRightTile);
    }));


  public SolrResource(OccurrenceHeatmapsService solrService, int tileSize, int bufferSize) {
    this.tileSize = tileSize;
    this.bufferSize = bufferSize;
    this.solrService = solrService;
    projection = Tiles.fromEPSG(EPSG_4326, tileSize);
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
    @DefaultValue(EPSG_4326) @QueryParam("srs") String srs,
    @QueryParam("bin") String bin,
    @DefaultValue(DEFAULT_HEX_PER_TILE) @QueryParam("hexPerTile") int hexPerTile,
    @DefaultValue(DEFAULT_SQUARE_SIZE) @QueryParam("squareSize") int squareSize,
    @Context HttpServletResponse response,
    @Context HttpServletRequest request
    ) throws Exception {
    enableCORS(response);
    Preconditions.checkArgument(EPSG_4326.equalsIgnoreCase(srs),
                                "Adhoc search maps are currently only available in EPSG:4326");
    OccurrenceHeatmapRequest heatmapRequest = OccurrenceHeatmapRequestProvider.buildOccurrenceHeatmapRequest(request);


    Preconditions.checkArgument(bin == null || BIN_MODE_HEX.equalsIgnoreCase(bin)
        || BIN_MODE_SQUARE.equalsIgnoreCase(bin), "Unsupported bin mode");

    // Note (Tim R): by testing in production index, we determine that 4 is a sensible performance choice
    // every 4 zoom levels the grid resolution increases
    //heatmapRequest.setZoom(z); // default behavior
    int solrLevel = (z/4) * 4;
    heatmapRequest.setZoom(solrLevel);

    heatmapRequest.setGeometry(solrSearchGeom(z, x, y));
    LOG.info("SOLR request:{}", heatmapRequest);

    OccurrenceHeatmapResponse solrResponse = solrService.searchHeatMap(heatmapRequest);
    VectorTileEncoder encoder = new VectorTileEncoder(tileSize, bufferSize, false);

    // Handle datelines in the SOLRResponse.
    OccurrenceHeatmapResponse datelineAdjustedResponse = datelineAdjustedResponse(solrResponse);

    // iterate the data structure from SOLR painting cells
    // (note: this is not pretty, but neither is the result from SOLR... some cleanup here would be beneficial)
    final List<List<Integer>> countsInts = datelineAdjustedResponse.getCountsInts2D();

    if (countsInts == null || countsInts.isEmpty()) {
      return encoder.encode();
    }
    for (int row = 0; row < countsInts.size(); row++) {
      if (countsInts.get(row) != null) {
        for (int column = 0; column < countsInts.get(row).size(); column++) {
          Integer count = countsInts.get(row).get(column);
          if (count != null && count > 0) {
            ColRow colRow = ColRow.of(column, row, z, x, y, datelineAdjustedResponse);

            // get the extent of the cell
            Bbox2D bbox2D = colRowToBbox.get(colRow);

            // for binning, we add the cell center point, otherwise the geometry
            encoder.addFeature("occurrence", Collections.singletonMap("total", count),
                               Objects.nonNull(bin)? bbox2D.getCenter() : bbox2D.getPolygon());
          }
        }
      }
    }
    return encodeTile(bin, z, x, y, hexPerTile, squareSize, encoder.encode());
  }


  private byte[] encodeTile(String bin, int z, long x, long y, int hexPerTile, int squareSize, byte[] encodedTile) throws IOException  {
    if (BIN_MODE_HEX.equalsIgnoreCase(bin)) {
      // binning will throw IAE on no data, so code defensively
      HexBin binner = new HexBin(HEX_TILE_SIZE, hexPerTile);
      return binner.bin(encodedTile, z, x, y);
    } else if (BIN_MODE_SQUARE.equalsIgnoreCase(bin)) {
      SquareBin binner = new SquareBin(SQUARE_TILE_SIZE, squareSize);
      return binner.bin(encodedTile, z, x, y);
    } else {
      return encodedTile;
    }
  }

  private Long2D getTopLeftTile(Point2D cellSW, Point2D cellNE, int z, long x, long y) {
    // convert the lat,lng into pixel coordinates
    Double2D swGlobalXY = projection.toGlobalPixelXY(cellNE.getY(), cellSW.getX(), z);

    return Tiles.toTileLocalXY(swGlobalXY, TileSchema.WGS84_PLATE_CAREÉ, z, x, y, tileSize, bufferSize);
  }

  private Long2D getBottomRightTile(Point2D cellSW, Point2D cellNE, int z, long x, long y) {
    // convert the lat,lng into pixel coordinates
    Double2D neGlobalXY = projection.toGlobalPixelXY(cellSW.getY(), cellNE.getX(), z);

    return Tiles.toTileLocalXY(neGlobalXY, TileSchema.WGS84_PLATE_CAREÉ, z, x, y, tileSize, bufferSize);
  }


  /**
   * Returns a new object with the min and max X set correctly for dateline adjustment or the source if the dateline
   * was not crossed.
   */
  private OccurrenceHeatmapResponse datelineAdjustedResponse(OccurrenceHeatmapResponse source) {
    if (source.getMaxX() < source.getMinX()) {
      double minX = source.getMinX();
      double maxX = source.getMaxX();
      // the one closest to dateline will always be the one to adjust
      if (Math.cos(Math.toRadians(source.getMaxX())) < Math.cos(Math.toRadians(source.getMinX()))) {
        maxX += 360;
      } else {
        minX -= 360;
      }

      // need to return new object because it sets internal variables (e.g. length) in constructor
      return new OccurrenceHeatmapResponse(source.getColumns(), source.getRows(), source.getCount(), minX, maxX,
                                           source.getMinY(), source.getMaxY(), source.getCountsInts2D());
    } else {
      return source;
    }
  }


  /**
   * Returns a SOLR search string for the geometry in WGS84 CRS for the tile with a buffer.
   */
  private static String solrSearchGeom(int z, long x, long y) {
    Double2D[] boundary = bufferedTileBoundary(z, x, y);
    return "[" + boundary[0].getX() + " " + boundary[0].getY() + " TO " + boundary[1].getX() + " " + boundary[1].getY() + "]";
  }

  /**
   * For the given tile, returns the envelope for the tile, with a buffer.
   * @param z zoom
   * @param x tile X address
   * @param y tile Y address
   * @param adjustDateline If the envelope should compensate for the dateline handling (only needed for search request)
   * @return an envelope for the tile, with the appropriate buffer
   */
  @VisibleForTesting
  static Double2D[] bufferedTileBoundary(int z, long x, long y) {
    int tilesPerZoom = 1 << z;
    double degreesPerTile = 180d/tilesPerZoom;
    double bufferDegrees = SOLR_QUERY_BUFFER_PERCENTAGE * degreesPerTile;

    // the edges of the tile after buffering
    double minLng = (degreesPerTile * x) - 180 - bufferDegrees;
    double maxLng = minLng + degreesPerTile + (bufferDegrees * 2);

    double maxLat = 90 - (degreesPerTile * y) + bufferDegrees;
    double minLat = maxLat - degreesPerTile - 2 * bufferDegrees;

    // handle the dateline wrapping (for all zooms above 0, which needs special attention)

    // clip the extent (SOLR barfs otherwise)
    maxLat = Math.min(maxLat, 90);
    minLat = Math.max(minLat, -90);

    return new Double2D[] {new Double2D(minLng, minLat), new Double2D(maxLng, maxLat)};
  }


  /**
   * Utility class used to cache bboxes made for Solr heatmap responses.
   */
  private static class Bbox2D {

    private final Long2D topLeft;

    private final Long2D bottomRight;

    //Evaluated lazily
    private Point center;

    private Polygon polygon;


    private Bbox2D(Long2D topLeft, Long2D bottomRight) {
      this.topLeft = topLeft;
      this.bottomRight = bottomRight;
    }

    static Bbox2D of(Long2D topLeft, Long2D bottomRight) {
      return new Bbox2D(topLeft, bottomRight);
    }

    public Long2D getTopLeft() {
      return topLeft;
    }

    public Long2D getBottomRight() {
      return bottomRight;
    }

    Point getCenter() {
      if (Objects.isNull(center)) {
        double centerX = center(topLeft.getX(), bottomRight.getX());

        double centerY = center(topLeft.getY(), bottomRight.getY());
        // hack: use just the center points for each cell
        center = GEOMETRY_FACTORY.createPoint(new Coordinate(centerX, centerY));
      }
      return center;
    }

    Polygon getPolygon() {
      if (Objects.isNull(polygon)) {
       polygon = GEOMETRY_FACTORY.createPolygon(new Coordinate[] {
        new Coordinate(topLeft.getX(), topLeft.getY()),
        new Coordinate(bottomRight.getX(), topLeft.getY()),
        new Coordinate(bottomRight.getX(), bottomRight.getY()),
        new Coordinate(topLeft.getX(), bottomRight.getY()),
        new Coordinate(topLeft.getX(), topLeft.getY())
       });
      }
      return polygon;

    }

    private double center(long min, long max) {
      return min + (((double) max - min) / 2);
    }


    @Override
    public int hashCode() {
      return Objects.hash(topLeft, bottomRight);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass())  {
        return false;
      }
      Bbox2D bbox2D = (Bbox2D) o;
      return Objects.equals(topLeft, bbox2D.topLeft) &&
             Objects.equals(bottomRight, bbox2D.bottomRight);
    }
  }

  /**
   * Utility class used to cache solr Responses.
   */
  private static final class ColRow {

    private final int z;

    private final long x;

    private final long y;

    private final int row;

    private final int column;

    private final Rectangle2D.Double cell;

    private ColRow(int column, int row, int z, long x, long y, Rectangle2D.Double cell) {
      this.column = column;
      this.row = row;
      this.z = z;
      this.x = x;
      this.y = y;
      this.cell = cell;
    }

    static ColRow of(int column, int row, int z, long x, long y, OccurrenceHeatmapResponse response) {
      return new ColRow(column, row, z, x, y,
                        new Rectangle2D.Double(response.getMinLng(column), response.getMinLat(row),
                                           response.getMaxLng(column) - response.getMinLng(column),
                                            response.getMaxLat(row) - response.getMinLat(row)));
    }


    @Override
    public int hashCode() {
      return Objects.hash(row, column, z, x, y, cell);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ColRow colRow = (ColRow) o;
      return column == colRow.column && row == colRow.row && z == colRow.z && x == colRow.x && y == colRow.y
             && Objects.equals(cell, colRow.cell);
    }
  }
}
