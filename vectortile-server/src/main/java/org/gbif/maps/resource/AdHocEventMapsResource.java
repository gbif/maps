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

import org.gbif.maps.TileServerConfiguration;
import org.gbif.occurrence.search.cache.PredicateCacheService;
import org.gbif.occurrence.search.heatmap.es.OccurrenceHeatmapsEsService;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.codahale.metrics.annotation.Timed;

import io.swagger.v3.oas.annotations.Hidden;

import static org.gbif.maps.resource.Params.DEFAULT_HEX_PER_TILE;
import static org.gbif.maps.resource.Params.DEFAULT_SQUARE_SIZE;

/**
 * ElasticSearch as a vector tile service.
 * Note to developers: This class could benefit from some significant refactoring and cleanup.
 */
@RestController
@ConditionalOnExpression("${esEventConfiguration.enabled}")
@RequestMapping(
  value = "/event/adhoc"
)
public final class AdHocEventMapsResource extends AdHocMapsResource {

  @Autowired
  public AdHocEventMapsResource(@Qualifier("eventHeatmapsEsService") OccurrenceHeatmapsEsService searchHeatmapsService,
                                TileServerConfiguration configuration,
                                @Qualifier("eventPredicateCache") PredicateCacheService predicateCacheService) {
    super(searchHeatmapsService,
          predicateCacheService,
          configuration.getEsEventConfiguration().getTileSize(),
          configuration.getEsEventConfiguration().getBufferSize());
  }

  // Overridden only to hide it from the OpenAPI schema.
  @Hidden
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
