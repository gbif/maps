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

import static org.gbif.maps.resource.Params.DEFAULT_HEX_PER_TILE;
import static org.gbif.maps.resource.Params.DEFAULT_SQUARE_SIZE;

import com.codahale.metrics.annotation.Timed;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.gbif.maps.TileServerConfiguration;
import org.gbif.occurrence.search.cache.PredicateCacheService;
import org.gbif.search.heatmap.HeatmapRequest;
import org.gbif.search.heatmap.occurrence.OccurrenceHeatmapRequest;
import org.gbif.search.heatmap.occurrence.OccurrenceHeatmapRequestProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * ElasticSearch as a vector tile service. Note to developers: This class could benefit from some
 * significant refactoring and cleanup.
 */
@RestController
@ConditionalOnExpression("${esOccurrenceConfiguration.enabled}")
@RequestMapping(value = "/occurrence/adhoc")
public final class AdHocOccurrenceMapsResource
    extends AdHocMapsResource<OccurrenceHeatmapRequest> {

  @Autowired
  public AdHocOccurrenceMapsResource(
      @Qualifier("occurrenceHeatmapsEsService")
          org.gbif.search.heatmap.es.occurrence.OccurrenceHeatmapsEsService searchHeatmapsService,
      TileServerConfiguration configuration,
      @Qualifier("occurrencePredicateCache") PredicateCacheService predicateCacheService) {
    super(
        searchHeatmapsService,
        new OccurrenceHeatmapRequestProvider(predicateCacheService),
        configuration.getEsOccurrenceConfiguration().getTileSize(),
        configuration.getEsOccurrenceConfiguration().getBufferSize());
  }

  // Overridden only for the OpenAPI documentation.
  @Operation(
    operationId = "getAdHocTile",
    summary = "Ad-hoc search tile",
    description = "Retrieves a tile showing occurrence locations in [Mapbox Vector Tile format](https://www.mapbox.com/vector-tiles/)\n" +
      "\n" +
      "Tiles contain a single layer `occurrence`. Features in that layer are either points (default) or polygons " +
      "(if chosen). Each feature has a `total` value; that is the number of occurrences at that point or in the polygon.\n" +
      "\n" +
      "Any search parameter allowed by the [occurrence search](/en/openapi/v1/occurrence#tag/Searching-occurrences/operation/searchOccurrence) is supported."
  )
  @Tag(name = "Occurrence maps")
  @Parameters(
    value = {
      @Parameter(
        name = "tileBuffer",
        in = ParameterIn.QUERY,
        hidden = true
      ),
      @Parameter(
        name = "mode",
        in = ParameterIn.QUERY,
        description = "Sets the search mode.  `GEO_BOUNDS` is the default, and returns rectangles that bound all the " +
          "occurrences in each bin.  `GEO_CENTROID` instead returns a point at the weighted centroid of the bin.",
        schema = @Schema(implementation = HeatmapRequest.Mode.class)
      )
    }
  )
  @CommonOpenAPI.TileProjectionAndStyleParameters
  @CommonOpenAPI.BinningParameters
  @CommonOpenAPI.DensitySearchParameters
  @CommonOpenAPI.TileResponses
  @RequestMapping(
    method = RequestMethod.GET,
    value = "/{z}/{x}/{y}.mvt",
    produces = "application/x-protobuf"
  )
  @Timed
  @Override
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
    return super.all(z, x, y, srs, bin, hexPerTile, squareSize, tileBuffer, response, request);
  }
}
