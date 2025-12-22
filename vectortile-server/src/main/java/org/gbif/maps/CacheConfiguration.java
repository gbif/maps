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
package org.gbif.maps;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Optional;
import org.cache2k.Cache;
import org.cache2k.config.Cache2kConfig;
import org.cache2k.extra.spring.SpringCache2kCacheManager;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.maps.io.PointFeature;
import org.gbif.maps.resource.HBaseMaps;
import org.gbif.maps.config.ConfigUtils;
import org.gbif.occurrence.search.cache.DefaultInMemoryPredicateCacheService;
import org.gbif.occurrence.search.cache.PredicateCacheService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableCaching
public class CacheConfiguration {

  @ConfigurationProperties(prefix = "cache.predicates")
  @Bean
  public Cache2kConfig<Integer, Predicate> predicateCache2kConfig() {
    return new Cache2kConfig<>();
  }

  @ConfigurationProperties(prefix = "cache.tiles")
  @Bean
  public Cache2kConfig<HBaseMaps.TileKey, Optional<byte[]>> tileCache2kConfig() {
    return new Cache2kConfig<>();
  }


  @ConfigurationProperties(prefix = "cache.points")
  @Bean
  public  Cache2kConfig<String, Optional<PointFeature.PointFeatures>> pointsCache2kConfig() {
    return new Cache2kConfig<>();
  }

  @Bean
  public SpringCache2kCacheManager cacheManager() {
    return new SpringCache2kCacheManager();
  }

  @Bean("occurrencePredicateCache")
  @ConditionalOnExpression("${esOccurrenceConfiguration.enabled}")
  public PredicateCacheService occurrencePredicateCacheService(ObjectMapper objectMapper, Cache2kConfig<Integer, Predicate> cache2kConfig, SpringCache2kCacheManager cacheManager, MeterRegistry meterRegistry) {
    cacheManager.addCaches(b -> cache2kConfig.builder().manager(cacheManager.getNativeCacheManager()).name("occurrencePredicateCache"));
    Cache<Integer, Predicate> cache = cacheManager.getNativeCacheManager().getCache("occurrencePredicateCache");
    ConfigUtils.registerCacheMetrics(cache, meterRegistry);
    return new DefaultInMemoryPredicateCacheService(objectMapper, cache);
  }

  @Bean("eventPredicateCache")
  @ConditionalOnExpression("${esEventConfiguration.enabled}")
  public PredicateCacheService eventPredicateCacheService(ObjectMapper objectMapper, Cache2kConfig<Integer, Predicate> cache2kConfig, SpringCache2kCacheManager cacheManager, MeterRegistry meterRegistry) {
    cacheManager.addCaches(b -> cache2kConfig.builder().manager(cacheManager.getNativeCacheManager()).name("eventPredicateCache"));
    Cache<Integer, Predicate> cache = cacheManager.getNativeCacheManager().getCache("eventPredicateCache");
    ConfigUtils.registerCacheMetrics(cache,  meterRegistry);
    return new DefaultInMemoryPredicateCacheService(objectMapper, cache);
  }
}
