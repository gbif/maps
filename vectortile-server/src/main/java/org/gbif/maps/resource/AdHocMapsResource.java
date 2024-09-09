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

import org.gbif.api.model.predicate.Predicate;
import org.gbif.maps.common.bin.HexBin;
import org.gbif.maps.common.bin.SquareBin;
import org.gbif.maps.common.projection.Double2D;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.codahale.metrics.annotation.Timed;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import io.swagger.v3.oas.annotations.Hidden;
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
public class AdHocMapsResource {

  private static final Logger LOG = LoggerFactory.getLogger(AdHocMapsResource.class);
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
  private static final String LAYER_NAME = "occurrence";

  @VisibleForTesting
  static final String QUERY_BUFFER_PERCENTAGE = "0.125";  // 1/8th tile buffer all around, similar to the HBase maps
  @VisibleForTesting
  static final String EPSG_4326 = "EPSG:4326";

  private final int tileSize;
  private final int bufferSize;
  private final OccurrenceHeatmapsEsService searchHeatmapsService;
  private final PredicateCacheService predicateCacheService;
  private final OccurrenceHeatmapRequestProvider provider;

  public AdHocMapsResource(OccurrenceHeatmapsEsService searchHeatmapsService,
                           PredicateCacheService predicateCacheService,
                           int tileSize,
                           int bufferSize) {
    this.tileSize = tileSize;
    this.bufferSize = bufferSize;
    this.searchHeatmapsService = searchHeatmapsService;
    this.predicateCacheService = predicateCacheService;
    provider = new OccurrenceHeatmapRequestProvider(predicateCacheService);
  }

  @Hidden
  @PostMapping(value = "/predicate",
               consumes =  {MediaType.APPLICATION_JSON_VALUE},
               produces = {MediaType.APPLICATION_JSON_VALUE, "application/x-javascript"})
  public Integer hashPredicate(@Valid @RequestBody Predicate predicate) {
    return predicateCacheService.put(predicate);
  }

  @Hidden
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

    Preconditions.checkArgument(bin == null
                                || BIN_MODE_HEX.equalsIgnoreCase(bin)
                                || BIN_MODE_SQUARE.equalsIgnoreCase(bin), "Unsupported bin mode");

    TileProjection projection = Tiles.fromEPSG(srs, tileSize);
    TileSchema schema = TileSchema.fromSRS(srs);

    List<OccurrenceHeatmapRequest> heatmapRequests = new ArrayList<>();
    VectorTileEncoder encoder = new VectorTileEncoder(tileSize, bufferSize, false);

    if (projection.isPoleTile(z, x, y)) {
      // If the current request touches the pole, make four requests instead of using a buffer across the whole width of the world.
      long leftX = (z==1) ? 0 : ((x+1)/2)*2-1;
      long topY  = (z==1) ? 0 : ((y+1)/2)*2-1;
      for (long xx = leftX; xx <= leftX+1; xx++) {
        for (long yy = topY; yy <= topY+1; yy++) {
          OccurrenceHeatmapRequest heatmapRequest = provider.buildOccurrenceHeatmapRequest(request);
          ZXY zxy = new ZXY(z, xx, yy, tileBuffer);
          Double2D[] boundary = projection.tileBoundary(zxy.z, zxy.x, zxy.y, zxy.tileBuffer);
          heatmapRequest.setGeometry(searchGeom(boundary));
          heatmapRequest.setZoom(z);
          heatmapRequests.add(heatmapRequest);
          LOG.info("Pole request: {} {}/{}/{}→{}/{} {}", srs, z, x, y, xx, yy, heatmapRequest);
        }
      }
    } else {
      OccurrenceHeatmapRequest heatmapRequest = provider.buildOccurrenceHeatmapRequest(request);
      ZXY zxy = new ZXY(z, x, y, tileBuffer);
      Double2D[] boundary = projection.tileBoundary(zxy.z, zxy.x, zxy.y, zxy.tileBuffer);
      if (boundary[0].getX() == boundary[1].getX() || boundary[0].getY() == boundary[1].getY()) {
        LOG.info("Empty tile request: {} {}/{}/{}", srs, z, x, y);
        return encoder.encode();
      }
      heatmapRequest.setGeometry(searchGeom(boundary));
      heatmapRequest.setZoom(z);
      heatmapRequests.add(heatmapRequest);
      LOG.info("Request: {} {}/{}/{} {}", srs, z, x, y, heatmapRequest);
    }

    int totalFeatures = 0;
    if (OccurrenceHeatmapRequest.Mode.GEO_BOUNDS == heatmapRequests.get(0).getMode()) {
      for (OccurrenceHeatmapRequest heatmapRequest : heatmapRequests) {
        EsOccurrenceHeatmapResponse.GeoBoundsResponse occurrenceHeatmapResponse = searchHeatmapsService.searchHeatMapGeoBounds(heatmapRequest);
        int[] featureCount = {0};
        occurrenceHeatmapResponse.getBuckets().stream().filter(geoGridBucket -> geoGridBucket.getDocCount() > 0)
          .forEach(geoGridBucket -> {
            // convert the lat,lng into pixel coordinates
            Bbox2D bbox2D = toBbox(geoGridBucket.getCell().getBounds(), projection, schema, z, x, y);
            // When binning, we add the cell centre point
            // Otherwise we use the geometry, unless it's zero area as these would be skipped by the encoder
            encoder.addFeature(LAYER_NAME,
              Collections.singletonMap("total", geoGridBucket.getDocCount()),
              Objects.nonNull(bin) || bbox2D.getPolygon().getArea() == 0
                ? bbox2D.getCenter()
                : bbox2D.getPolygon()
            );
            featureCount[0]++;
          });
        totalFeatures += featureCount[0];
        if (featureCount[0] == OccurrenceHeatmapRequestProvider.DEFAULT_BUCKET_LIMIT) {
          LOG.warn("Maximum geohash feature limit ({}) reached for tile {} {}/{}/{} {}",
            OccurrenceHeatmapRequestProvider.DEFAULT_BUCKET_LIMIT, srs, z, x, y, heatmapRequest);
        }
      }

    } else if (OccurrenceHeatmapRequest.Mode.GEO_CENTROID == heatmapRequests.get(0).getMode()) {
      for (OccurrenceHeatmapRequest heatmapRequest : heatmapRequests) {
        EsOccurrenceHeatmapResponse.GeoCentroidResponse occurrenceHeatmapResponse = searchHeatmapsService.searchHeatMapGeoCentroid(heatmapRequest);
        int[] featureCount = {0};
        occurrenceHeatmapResponse.getBuckets().stream()
          .filter(geoGridBucket -> geoGridBucket.getDocCount() > 0)
          .forEach(geoGridBucket -> {
            // for binning, we add the cell centre point, and the geohash to allow for webgl clicking
            Map<String, Object> attributes = new HashMap<>(2);
            attributes.put("total", geoGridBucket.getDocCount());
            attributes.put("geohash", geoGridBucket.getKey());
            encoder.addFeature(LAYER_NAME, attributes,
              toPoint(geoGridBucket.getCentroid(), projection, schema, z, x, y));
            featureCount[0]++;
          });
        totalFeatures += featureCount[0];
        if (featureCount[0] == OccurrenceHeatmapRequestProvider.DEFAULT_BUCKET_LIMIT) {
          LOG.warn("Maximum geohash feature limit ({}) reached for tile {} {}/{}/{} {}",
            OccurrenceHeatmapRequestProvider.DEFAULT_BUCKET_LIMIT, srs, z, x, y, heatmapRequest);
        }
      }
    }

    if (totalFeatures == 0) {
      return encoder.encode();
    } else {
      return encodeTile(bin, z, x, y, hexPerTile, squareSize, encoder.encode());
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
  static String searchGeom(Double2D[] boundary) {
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

    final int z;

    final long x;

    final long y;

    final double tileBuffer;

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
