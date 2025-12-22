package org.gbif.maps.config;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.cache2k.Cache;
import org.cache2k.core.api.InternalCache;

public class ConfigUtils {

  // Manually expose Cache2K metrics using Micrometer
  public static void registerCacheMetrics(Cache<?, ?> cache, MeterRegistry meterRegistry) {
    // Obtain Cache2K's internal statistics
    InternalCache<?, ?> internalCache = cache.requestInterface(InternalCache.class);

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
        .description(
            "All triggered and tried refresh actions, including those that produced and exception as result")
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
