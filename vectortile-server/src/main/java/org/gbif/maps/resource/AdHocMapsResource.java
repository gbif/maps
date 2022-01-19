/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.maps.resource;

import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.maps.TileServerConfiguration;
import org.gbif.maps.common.bin.HexBin;
import org.gbif.maps.common.bin.SquareBin;
import org.gbif.maps.common.projection.Double2D;
import org.gbif.maps.common.projection.Int2D;
import org.gbif.maps.common.projection.Long2D;
import org.gbif.maps.common.projection.TileProjection;
import org.gbif.maps.common.projection.TileSchema;
import org.gbif.maps.common.projection.Tiles;
import org.gbif.occurrence.search.cache.PredicateCacheService;
import org.gbif.occurrence.search.heatmap.OccurrenceHeatmapRequest;
import org.gbif.occurrence.search.heatmap.OccurrenceHeatmapRequestProvider;
import org.gbif.occurrence.search.heatmap.es.EsOccurrenceHeatmapResponse;
import org.gbif.occurrence.search.heatmap.es.OccurrenceHeatmapsEsService;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.codahale.metrics.annotation.Timed;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

import no.ecc.vectortile.VectorTileEncoder;

import static org.gbif.maps.resource.Params.BIN_MODE_HEX;
import static org.gbif.maps.resource.Params.BIN_MODE_SQUARE;
import static org.gbif.maps.resource.Params.DEFAULT_HEX_PER_TILE;
import static org.gbif.maps.resource.Params.DEFAULT_SQUARE_SIZE;
import static org.gbif.maps.resource.Params.HEX_TILE_SIZE;
import static org.gbif.maps.resource.Params.SQUARE_TILE_SIZE;
import static org.gbif.maps.resource.Params.enableCORS;

/**
 * ElasticSearch as a vector tile service.
 * Note to developers: This class could benefit from some significant refactoring and cleanup.
 */
@RestController
@RequestMapping(
  value = "/occurrence/adhoc"
)
public final class AdHocMapsResource {

  private static final Logger LOG = LoggerFactory.getLogger(AdHocMapsResource.class);
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
  private static final String LAYER_NAME = "occurrence";

  @VisibleForTesting
  static final String QUERY_BUFFER_PERCENTAGE = "0.125";  // 1/8th tile buffer all around, similar to the HBase maps
  private static final String EPSG_3857 = "EPSG:3857";
  private static final String EPSG_4326 = "EPSG:4326";

  private final int tileSize;
  private final int bufferSize;
  private final OccurrenceHeatmapsEsService searchHeatmapsService;
  private final PredicateCacheService predicateCacheService;
  private final OccurrenceHeatmapRequestProvider provider;

  @Autowired
  public AdHocMapsResource(OccurrenceHeatmapsEsService searchHeatmapsService, TileServerConfiguration configuration,
                           PredicateCacheService predicateCacheService) {
    this.tileSize = configuration.getEsConfiguration().getTileSize();
    this.bufferSize = configuration.getEsConfiguration().getBufferSize();
    this.searchHeatmapsService = searchHeatmapsService;
    this.predicateCacheService = predicateCacheService;
    provider = new OccurrenceHeatmapRequestProvider(predicateCacheService);
  }

  @PostMapping(value = "/predicate",
               consumes =  {MediaType.APPLICATION_JSON_VALUE},
               produces = {MediaType.APPLICATION_JSON_VALUE, "application/x-javascript"})
  public Integer hashPredicate(@Valid @RequestBody Predicate predicate) {
    return predicateCacheService.put(predicate);
  }

  @GetMapping(value = "/predicate/{predicateHash}",
    produces = {MediaType.APPLICATION_JSON_VALUE, "application/x-javascript"})
  public ResponseEntity<Predicate> getPredicate(@Valid @PathVariable("predicateHash") Integer predicateHash) {
    return Optional.ofNullable(predicateCacheService.get(predicateHash))
      .map(ResponseEntity::ok)
      .orElse(ResponseEntity.notFound().build());
  }

  @RequestMapping(
    method = RequestMethod.GET,
    value = "/{z}/{x}/{y}.mvt",
    produces = "application/x-protobuf"
  )
  @Timed
  public byte[] all(
    @PathVariable("z") int z,
    @PathVariable("x") long x,
    @PathVariable("y") long y,
    @RequestParam(value = "srs", defaultValue = EPSG_4326) String srs,
    @RequestParam(value = "bin", required = false) String bin,
    @RequestParam(value = "hexPerTile", defaultValue = DEFAULT_HEX_PER_TILE) int hexPerTile,
    @RequestParam(value = "squareSize", defaultValue = DEFAULT_SQUARE_SIZE) int squareSize,
    @RequestParam(value = "tileBuffer", defaultValue = QUERY_BUFFER_PERCENTAGE) double tileBuffer,
    HttpServletResponse response,
    HttpServletRequest request
    ) throws Exception {
    enableCORS(response);
    Preconditions.checkArgument(EPSG_4326.equalsIgnoreCase(srs) || EPSG_3857.equalsIgnoreCase(srs),
                                "Adhoc search maps are currently only available in EPSG:4326 and EPSG:3857");
    OccurrenceHeatmapRequest heatmapRequest = provider.buildOccurrenceHeatmapRequest(request);

    Preconditions.checkArgument(bin == null
                                || BIN_MODE_HEX.equalsIgnoreCase(bin)
                                || BIN_MODE_SQUARE.equalsIgnoreCase(bin), "Unsupported bin mode");

    TileProjection projection = Tiles.fromEPSG(srs, tileSize);
    TileSchema schema = TileSchema.fromSRS(srs);

    heatmapRequest.setGeometry(searchGeom(projection, new ZXY(z, x, y, tileBuffer)));
    heatmapRequest.setZoom(z);

    LOG.info("Request:{}", heatmapRequest);

    VectorTileEncoder encoder = new VectorTileEncoder(tileSize, bufferSize, false);

    if (OccurrenceHeatmapRequest.Mode.GEO_BOUNDS == heatmapRequest.getMode()) {
      EsOccurrenceHeatmapResponse.GeoBoundsResponse occurrenceHeatmapResponse = searchHeatmapsService.searchHeatMapGeoBounds(heatmapRequest);
      if (occurrenceHeatmapResponse.getBuckets().isEmpty()) {
        return encoder.encode();
      }
      occurrenceHeatmapResponse.getBuckets().stream().filter(geoGridBucket -> geoGridBucket.getDocCount() > 0)
        .forEach(geoGridBucket -> {
          // convert the lat,lng into pixel coordinates
          Bbox2D bbox2D = toBbox(geoGridBucket.getCell().getBounds(), projection, schema, z, x, y);
          // for binning, we add the cell center point, otherwise the geometry
          encoder.addFeature(LAYER_NAME,
            Collections.singletonMap("total", geoGridBucket.getDocCount()),
            Objects.nonNull(bin) ? bbox2D.getCenter() : bbox2D.getPolygon());
        });

    } else if (OccurrenceHeatmapRequest.Mode.GEO_CENTROID == heatmapRequest.getMode()) {
      EsOccurrenceHeatmapResponse.GeoCentroidResponse occurrenceHeatmapResponse = searchHeatmapsService.searchHeatMapGeoCentroid(heatmapRequest);
      if (occurrenceHeatmapResponse.getBuckets().isEmpty()) {
        return encoder.encode();
      }
      occurrenceHeatmapResponse.getBuckets().stream().filter(geoGridBucket -> geoGridBucket.getDocCount() > 0)
        .forEach(geoGridBucket -> {

          // for binning, we add the cell center point, and the geohash to allow for webgl clicking
          Map<String, Object> attributes = Collections.singletonMap("total", geoGridBucket.getDocCount());
          attributes.put("geohash", geoGridBucket.getKey());
          encoder.addFeature(LAYER_NAME, attributes,
            toPoint(geoGridBucket.getCentroid(), projection, schema, z, x, y));
        });
    }

    return encodeTile(bin, z, x, y, hexPerTile, squareSize, encoder.encode());
  }

  private void checkPredicateHashParam(HttpServletRequest httpServletRequest) {
    String predicateHashParam = httpServletRequest.getParameter(OccurrenceHeatmapRequestProvider.PARAM_PREDICATE_HASH);
    if (!Strings.isNullOrEmpty(predicateHashParam)) {
      Predicate predicate = predicateCacheService.get(Integer.parseInt(predicateHashParam));
    }
  }


  /**
   * Translates the bounds into a Bbox2D.
   */
  private Bbox2D toBbox(EsOccurrenceHeatmapResponse.Bounds bounds, TileProjection projection, TileSchema schema, int z, long x, long y) {
    Double2D swGlobalXY = projection.toGlobalPixelXY(bounds.getTopLeft().getLat(), bounds.getTopLeft().getLon(), z);
    Long2D swTileXY = Tiles.toTileLocalXY(swGlobalXY, schema, z, x, y, tileSize, bufferSize);

    Double2D neGlobalXY = projection.toGlobalPixelXY(bounds.getBottomRight().getLat(), bounds.getBottomRight().getLon(), z);
    Long2D neTileXY = Tiles.toTileLocalXY(neGlobalXY, schema, z, x, y, tileSize, bufferSize);

    return Bbox2D.of(swTileXY, neTileXY);
  }

  /**
    * Translates the coordinate into a Point.
    */
  private Point toPoint(EsOccurrenceHeatmapResponse.Coordinate coordinate, TileProjection projection, TileSchema schema, int z, long x, long y) {
    Double2D globalXY = projection.toGlobalPixelXY(coordinate.getLat(), coordinate.getLon(), z);
    Long2D tileXY = Tiles.toTileLocalXY(globalXY, schema, z, x, y, tileSize, bufferSize);

    return GEOMETRY_FACTORY.createPoint(new Coordinate(tileXY.getX(), tileXY.getY()));
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
  @VisibleForTesting
  static String searchGeom(TileProjection projection, ZXY zxy) {
    Double2D[] boundary = projection.tileBoundary(zxy.z, zxy.x, zxy.y, zxy.tileBuffer);
    return boundary[0].getX() + "," + boundary[0].getY() + "," + boundary[1].getX() + "," + boundary[1].getY();
  }

  /**
   * Utility class for bounding boxes made for heatmap responses.
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
      if (!(o instanceof Bbox2D)) {
        return false;
      }
      Bbox2D bbox2D = (Bbox2D) o;
      return Objects.equals(topLeft, bbox2D.topLeft) &&
             Objects.equals(bottomRight, bbox2D.bottomRight);
    }
  }

  /**
   * Utility class used for z,x,y used to calculate tile/bboxes.
   */
  @VisibleForTesting
  static final class ZXY {

    private final int z;

    private final long x;

    private final long y;

    private final double tileBuffer;

    ZXY(int z, long x, long y, double tileBuffer) {
      this.z = z;
      this.x = x;
      this.y = y;
      this.tileBuffer = tileBuffer;
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
      return z == zxy.z && x == zxy.x && y == zxy.y && tileBuffer == zxy.tileBuffer;
    }

    @Override
    public int hashCode() {
      return Objects.hash(z, x, y);
    }
  }
}
