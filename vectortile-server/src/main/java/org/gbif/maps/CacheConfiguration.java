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

import org.gbif.api.model.predicate.Predicate;
import org.gbif.occurrence.search.cache.DefaultInMemoryPredicateCacheService;
import org.gbif.occurrence.search.cache.PredicateCacheService;

import org.cache2k.config.Cache2kConfig;
import org.cache2k.extra.spring.SpringCache2kCacheManager;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.databind.ObjectMapper;

@Configuration
@EnableCaching
public class CacheConfiguration {

  @ConfigurationProperties(prefix = "cache.predicates")
  @Bean
  public Cache2kConfig<Integer, Predicate> predicateCache2kConfig() {
    return new Cache2kConfig<>();
  }

  @Bean
  public SpringCache2kCacheManager cacheManager() {
    return new SpringCache2kCacheManager();
  }

  @Bean("occurrencePredicateCache")
  @ConditionalOnExpression("${esOccurrenceConfiguration.enabled}")
  public PredicateCacheService occurrencePredicateCacheService(ObjectMapper objectMapper, Cache2kConfig<Integer, Predicate> cache2kConfig, SpringCache2kCacheManager cacheManager) {
    cacheManager.addCaches(b -> cache2kConfig.builder().manager(cacheManager.getNativeCacheManager()).name("occurrencePredicateCache2k"));
    return new DefaultInMemoryPredicateCacheService(objectMapper, cacheManager.getNativeCacheManager().getCache("occurrencePredicateCache2k"));
  }

  @Bean("eventPredicateCache")
  @ConditionalOnExpression("${esEventConfiguration.enabled}")
  public PredicateCacheService eventPredicateCacheService(ObjectMapper objectMapper, Cache2kConfig<Integer, Predicate> cache2kConfig, SpringCache2kCacheManager cacheManager) {
    cacheManager.addCaches(b -> cache2kConfig.builder().manager(cacheManager.getNativeCacheManager()).name("eventPredicateCache2k"));
    return new DefaultInMemoryPredicateCacheService(objectMapper, cacheManager.getNativeCacheManager().getCache("occurrencePredicateCache2k"));
  }
}
