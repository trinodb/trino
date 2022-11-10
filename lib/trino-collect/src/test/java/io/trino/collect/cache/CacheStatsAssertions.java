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
package io.trino.collect.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheStats;

import java.util.concurrent.Callable;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class CacheStatsAssertions
{
    public static CacheStatsAssertions assertCacheStats(Cache<?, ?> cache)
    {
        requireNonNull(cache, "cache is null");
        return assertCacheStats(cache::stats);
    }

    public static CacheStatsAssertions assertCacheStats(Supplier<CacheStats> statsSupplier)
    {
        return new CacheStatsAssertions(statsSupplier);
    }

    private final Supplier<CacheStats> stats;

    private long loads;
    private long hits;
    private long misses;

    private CacheStatsAssertions(Supplier<CacheStats> stats)
    {
        this.stats = requireNonNull(stats, "stats is null");
    }

    public CacheStatsAssertions loads(long value)
    {
        this.loads = value;
        return this;
    }

    public CacheStatsAssertions hits(long value)
    {
        this.hits = value;
        return this;
    }

    public CacheStatsAssertions misses(long value)
    {
        this.misses = value;
        return this;
    }

    public void afterRunning(Runnable runnable)
    {
        try {
            calling(() -> {
                runnable.run();
                return null;
            });
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public <T> T calling(Callable<T> callable)
            throws Exception
    {
        CacheStats beforeStats = stats.get();
        T value = callable.call();
        CacheStats afterStats = stats.get();

        long loadDelta = afterStats.loadCount() - beforeStats.loadCount();
        long missesDelta = afterStats.missCount() - beforeStats.missCount();
        long hitsDelta = afterStats.hitCount() - beforeStats.hitCount();

        assertThat(loadDelta).as("loads (delta)").isEqualTo(loads);
        assertThat(hitsDelta).as("hits (delta)").isEqualTo(hits);
        assertThat(missesDelta).as("misses (delta)").isEqualTo(misses);

        return value;
    }
}
