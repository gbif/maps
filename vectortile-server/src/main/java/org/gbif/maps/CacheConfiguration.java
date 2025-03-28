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
import org.gbif.maps.io.PointFeature;
import org.gbif.maps.resource.HBaseMaps;
import org.gbif.occurrence.search.cache.DefaultInMemoryPredicateCacheService;
import org.gbif.occurrence.search.cache.PredicateCacheService;

import org.cache2k.Cache;
import org.cache2k.config.Cache2kConfig;
import org.cache2k.core.api.InternalCache;
import org.cache2k.extra.spring.SpringCache2kCacheManager;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.Optional;

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
    registerCacheMetrics(cache, meterRegistry);
    return new DefaultInMemoryPredicateCacheService(objectMapper, cache);
  }

  @Bean("eventPredicateCache")
  @ConditionalOnExpression("${esEventConfiguration.enabled}")
  public PredicateCacheService eventPredicateCacheService(ObjectMapper objectMapper, Cache2kConfig<Integer, Predicate> cache2kConfig, SpringCache2kCacheManager cacheManager, MeterRegistry meterRegistry) {
    cacheManager.addCaches(b -> cache2kConfig.builder().manager(cacheManager.getNativeCacheManager()).name("eventPredicateCache"));
    Cache<Integer, Predicate> cache = cacheManager.getNativeCacheManager().getCache("eventPredicateCache");
    registerCacheMetrics(cache,  meterRegistry);
    return new DefaultInMemoryPredicateCacheService(objectMapper, cache);
  }

  // Manually expose Cache2K metrics using Micrometer
  public static void registerCacheMetrics(Cache<?, ?> cache, MeterRegistry meterRegistry) {
    // Obtain Cache2K's internal statistics
    InternalCache<?,?> internalCache = cache.requestInterface(InternalCache.class);

    // Register custom Micrometer gauges for cache metrics
    Gauge.builder("cache.size", internalCache, InternalCache::getTotalEntryCount)
      .description("The number of entries in the cache")
      .tags("cache", cache.getName())
      .register(meterRegistry);

    Gauge.builder("cache.gets", internalCache, c -> c.getInfo().getGetCount())
      .description("The number of cache gets (hits + misses)")
      .tags("cache", cache.getName())
      .register(meterRegistry);

    Gauge.builder("cache.hits", internalCache, c -> c.getInfo().getHeapHitCount())
      .description("The number of cache hits")
      .tags("cache", cache.getName())
      .register(meterRegistry);

    Gauge.builder("cache.misses", internalCache, c -> c.getInfo().getMissCount())
      .description("The number of cache misses")
      .tags("cache", cache.getName())
      .register(meterRegistry);

    Gauge.builder("cache.evictions", internalCache, c -> c.getInfo().getEvictedCount())
      .description("The number of cache evictions")
      .tags("cache", cache.getName())
      .register(meterRegistry);

    Gauge.builder("cache.hit_rate", internalCache, c -> c.getInfo().getHitRate())
      .description("Hit rate for this cache")
      .tags("cache", cache.getName())
      .register(meterRegistry);

    Gauge.builder("cache.load_count", internalCache, c -> c.getInfo().getLoadCount())
      .description("Successful loads including reloads and refresh")
      .tags("cache", cache.getName())
      .register(meterRegistry);

    Gauge.builder("cache.refresh_hit_count", internalCache, c -> c.getInfo().getRefreshedHitCount())
      .description("All triggered and tried refresh actions, including those that produced and exception as result")
      .tags("cache", cache.getName())
      .register(meterRegistry);

    Gauge.builder("cache.expired_count", internalCache, c -> c.getInfo().getExpiredCount())
      .description("Counts entries that expired")
      .tags("cache", cache.getName())
      .register(meterRegistry);

    Gauge.builder("cache.millis_per_load", internalCache, c -> c.getInfo().getMillisPerLoad())
      .description("Counts entries that expired")
      .tags("cache", cache.getName())
      .register(meterRegistry);

    Gauge.builder("cache.heap_capacity", internalCache, c -> c.getInfo().getHeapCapacity())
      .description("Configured limit of the total cache entry capacity or -1 if weigher is used")
      .tags("cache", cache.getName())
      .register(meterRegistry);
  }
}
