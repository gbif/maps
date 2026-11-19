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

import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.predicate.EqualsPredicate;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.occurrence.search.cache.DefaultInMemoryPredicateCacheService;
import org.gbif.occurrence.search.cache.PredicateCacheService;
import org.gbif.search.heatmap.HeatmapRequestProvider;
import org.gbif.search.heatmap.occurrence.OccurrenceHeatmapRequest;

import org.cache2k.config.Cache2kConfig;
import org.junit.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import jakarta.servlet.http.HttpServletRequest;

import static org.junit.Assert.assertEquals;

public class PredicateCacheTest {

  private static AdHocMapsResource<OccurrenceHeatmapRequest> resource() {
    PredicateCacheService cache = DefaultInMemoryPredicateCacheService.getInstance(new Cache2kConfig<>());
    HeatmapRequestProvider<OccurrenceHeatmapRequest> provider = new HeatmapRequestProvider<OccurrenceHeatmapRequest>() {
      @Override
      public OccurrenceHeatmapRequest buildOccurrenceHeatmapRequest(HttpServletRequest request) {
        throw new UnsupportedOperationException();
      }

      @Override
      public PredicateCacheService getPredicateCacheService() {
        return cache;
      }
    };
    return new AdHocMapsResource<>(null, provider, 4096, 64);
  }

  private static Predicate taxonKey(String value) {
    return new EqualsPredicate<>(OccurrenceSearchParameter.TAXON_KEY, value, false);
  }

  @Test
  public void testSamePredicateReturnsSameHash() {
    AdHocMapsResource<OccurrenceHeatmapRequest> resource = resource();
    ResponseEntity<Integer> first = resource.hashPredicate(taxonKey("212"));
    ResponseEntity<Integer> second = resource.hashPredicate(taxonKey("212"));
    assertEquals(HttpStatus.OK, first.getStatusCode());
    assertEquals(first.getBody(), second.getBody());
  }

  @Test
  public void testHashCollisionIsRejected() {
    AdHocMapsResource<OccurrenceHeatmapRequest> resource = resource();
    // "Aa" and "BB" have the same String.hashCode(), so these predicates have the same cache key
    ResponseEntity<Integer> first = resource.hashPredicate(taxonKey("Aa"));
    ResponseEntity<Integer> second = resource.hashPredicate(taxonKey("BB"));
    assertEquals(HttpStatus.OK, first.getStatusCode());
    assertEquals(HttpStatus.CONFLICT, second.getStatusCode());
  }
}
