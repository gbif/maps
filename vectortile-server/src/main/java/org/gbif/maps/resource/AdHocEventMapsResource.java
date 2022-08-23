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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

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
}
