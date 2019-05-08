package org.gbif.maps.resource;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.gbif.maps.common.bin.HexBin;
import org.gbif.maps.common.bin.SquareBin;
import org.gbif.maps.common.projection.Double2D;
import org.gbif.maps.common.projection.Long2D;
import org.gbif.maps.common.projection.TileProjection;
import org.gbif.maps.common.projection.TileSchema;
import org.gbif.maps.common.projection.Tiles;
import org.gbif.occurrence.search.heatmap.OccurrenceHeatmapRequest;
import org.gbif.occurrence.search.heatmap.OccurrenceHeatmapRequestProvider;

import java.io.IOException;
import java.util.Collections;
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
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import no.ecc.vectortile.VectorTileEncoder;
import org.gbif.occurrence.search.heatmap.es.EsOccurrenceHeatmapResponse;
import org.gbif.occurrence.search.heatmap.es.OccurrenceHeatmapsEsService;
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
public final class AddHocMapsResource {

  private static final Logger LOG = LoggerFactory.getLogger(AddHocMapsResource.class);
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
  private static final String LAYER_NAME = "occurrence";

  //Experimental: keeps a cache of all the calculated geometries from z,x and y.
  private static final LoadingCache<ZXY,String> ZXY_TO_GEOM = CacheBuilder.newBuilder().build(CacheLoader.from(AddHocMapsResource::searchGeom));

  @VisibleForTesting
  static final double QUERY_BUFFER_PERCENTAGE = 0.125;  // 1/8th tile buffer all around, similar to the HBase maps
  private static final String EPSG_4326 = "EPSG:4326";

  private final int tileSize;
  private final int bufferSize;
  private final TileProjection projection;
  private final OccurrenceHeatmapsEsService searchHeatmapsService;


  public AddHocMapsResource(OccurrenceHeatmapsEsService searchHeatmapsService, int tileSize, int bufferSize) {
    this.tileSize = tileSize;
    this.bufferSize = bufferSize;
    this.searchHeatmapsService = searchHeatmapsService;
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


    Preconditions.checkArgument(bin == null
                                || BIN_MODE_HEX.equalsIgnoreCase(bin)
                                || BIN_MODE_SQUARE.equalsIgnoreCase(bin), "Unsupported bin mode");

    heatmapRequest.setGeometry(ZXY_TO_GEOM.get(new ZXY(z, x, y)));
    heatmapRequest.setZoom(z);

    LOG.info("Request:{}", heatmapRequest);

    VectorTileEncoder encoder = new VectorTileEncoder(tileSize, bufferSize, false);

    if (OccurrenceHeatmapRequest.Mode.GEO_BOUNDS == heatmapRequest.getMode()) {
      EsOccurrenceHeatmapResponse.GeoBoundsResponse occurrenceHeatmapResponse = searchHeatmapsService.searchHeatMapGeoBounds(heatmapRequest);

      occurrenceHeatmapResponse.getBuckets().stream().filter(geoGridBucket -> geoGridBucket.getDocCount() > 0)
        .forEach(geoGridBucket -> {
          // convert the lat,lng into pixel coordinates
          Bbox2D bbox2D = toBbox(geoGridBucket.getCell().getBounds(), z, x, y);
          // for binning, we add the cell center point, otherwise the geometry
          encoder.addFeature(LAYER_NAME, Collections.singletonMap("total", geoGridBucket.getDocCount()),
            Objects.nonNull(bin) ? bbox2D.getCenter() : bbox2D.getPolygon());
        });
    } else if(OccurrenceHeatmapRequest.Mode.GEO_CENTROID == heatmapRequest.getMode()) {
      EsOccurrenceHeatmapResponse.GeoCentroidResponse occurrenceHeatmapResponse = searchHeatmapsService.searchHeatMapGeoCentroid(heatmapRequest);

      occurrenceHeatmapResponse.getBuckets().stream().filter(geoGridBucket -> geoGridBucket.getDocCount() > 0)
        .forEach(geoGridBucket -> {
          // for binning, we add the cell center point, otherwise the geometry
          encoder.addFeature(LAYER_NAME,
                            Collections.singletonMap("total", geoGridBucket.getDocCount()),
                            toPoint(geoGridBucket.getCentroid(), z,x, y));
        });
    }

    return encodeTile(bin, z, x, y, hexPerTile, squareSize, encoder.encode());
  }


  /**
   * Translates the bounds into a Bbox2D.
   */
  private Bbox2D toBbox(EsOccurrenceHeatmapResponse.Bounds bounds, int z, long x, long y) {
    Double2D swGlobalXY = projection.toGlobalPixelXY(bounds.getTopLeft().getLat(), bounds.getTopLeft().getLon(), z);
    Long2D swTileXY = Tiles.toTileLocalXY(swGlobalXY, TileSchema.WGS84_PLATE_CAREÉ, z, x, y, tileSize, bufferSize);

    Double2D neGlobalXY = projection.toGlobalPixelXY(bounds.getBottomRight().getLat(), bounds.getBottomRight().getLon(), z);
    Long2D neTileXY = Tiles.toTileLocalXY(neGlobalXY, TileSchema.WGS84_PLATE_CAREÉ, z, x, y, tileSize, bufferSize);

    return Bbox2D.of(swTileXY, neTileXY);
  }

  /**
    * Translates the coordinate into a Point.
    */
  private Point toPoint(EsOccurrenceHeatmapResponse.Coordinate coordinate, int z, long x, long y) {
    Double2D swGlobalXY = projection.toGlobalPixelXY(coordinate.getLat(), coordinate.getLon(), z);
    Long2D swTileXY = Tiles.toTileLocalXY(swGlobalXY, TileSchema.WGS84_PLATE_CAREÉ, z, x, y, tileSize, bufferSize);
    return GEOMETRY_FACTORY.createPoint(new Coordinate(swTileXY.getX(), swTileXY.getY()));
  }


  /**
   * Performs the tile encoding.
   */
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

  /**
   * Returns a BBox search string for the geometry in WGS84 CRS for the tile with a buffer.
   */
  private static String searchGeom(ZXY zxy) {
    Double2D[] boundary = bufferedTileBoundary(zxy.z, zxy.x, zxy.y);
    return boundary[0].getX() + "," + boundary[0].getY() + "," + boundary[1].getX() + "," + boundary[1].getY();
  }

  /**
   * For the given tile, returns the envelope for the tile, with a buffer.
   * @param z zoom
   * @param x tile X address
   * @param y tile Y address
   * @return an envelope for the tile, with the appropriate buffer
   */
  @VisibleForTesting
  static Double2D[] bufferedTileBoundary(int z, long x, long y) {
    int tilesPerZoom = 1 << z;
    double degreesPerTile = 180d/tilesPerZoom;
    double bufferDegrees = QUERY_BUFFER_PERCENTAGE * degreesPerTile;

    // the edges of the tile after buffering
    double minLng = to180Degrees((degreesPerTile * x) - 180 - bufferDegrees);
    double maxLng = to180Degrees(minLng + degreesPerTile + (bufferDegrees * 2));

    // clip the extent (ES barfs otherwise)
    double maxLat = Math.min(90 - (degreesPerTile * y) + bufferDegrees, 90);
    double minLat = Math.max(maxLat - degreesPerTile - 2 * bufferDegrees, -90);

    return new Double2D[] {new Double2D(minLng, minLat), new Double2D(maxLng, maxLat)};
  }



  /**
   * if the longitude is expressed from 0..360 it is converted to -180..180.
   */
  @VisibleForTesting
  static double to180Degrees(double longitude) {
    if(longitude > 180) {
      return longitude - 360;
    } else if (longitude < -180){
      return longitude + 360;
    }
    return longitude;
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
   * Utility class used to cache z,x,y values constantly used to calculate tile/bboxes.
   */
  private static final class ZXY {

    private final int z;

    private final long x;

    private final long y;

    ZXY(int z, long x, long y) {
      this.z = z;
      this.x = x;
      this.y = y;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ZXY zxy = (ZXY) o;
      return z == zxy.z && x == zxy.x && y == zxy.y;
    }

    @Override
    public int hashCode() {
      return Objects.hash(z, x, y);
    }
  }
}
