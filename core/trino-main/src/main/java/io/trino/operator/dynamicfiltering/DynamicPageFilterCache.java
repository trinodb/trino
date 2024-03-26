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
package io.trino.operator.dynamicfiltering;

import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.jmx.CacheStatsMBean;
import io.trino.cache.NonEvictableCache;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.type.TypeOperators;
import jakarta.annotation.PreDestroy;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MINUTES;

public class DynamicPageFilterCache
{
    private static final int COLLECTION_THREADS = 4;

    private final TypeOperators typeOperators;
    private final ExecutorService executor;
    private final NonEvictableCache<CacheKey, DynamicPageFilter> cache;
    private final IsolatedBlockFilterFactory isolatedBlockFilterFactory;

    @Inject
    public DynamicPageFilterCache(TypeOperators typeOperators)
    {
        this.typeOperators = requireNonNull(typeOperators, "typeManager is null");
        this.executor = newFixedThreadPool(COLLECTION_THREADS, daemonThreadsNamed("dynamic-page-filter-collector-%s"));
        this.cache = buildNonEvictableCache(CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterAccess(2, MINUTES)
                .recordStats());
        this.isolatedBlockFilterFactory = new IsolatedBlockFilterFactory();
    }

    @PreDestroy
    public void shutdown()
    {
        executor.shutdownNow();
    }

    @Managed
    @Nested
    public CacheStatsMBean getDynamicPageFilterCacheStats()
    {
        return new CacheStatsMBean(cache);
    }

    public DynamicPageFilter getDynamicPageFilter(DynamicFilter dynamicFilter, Map<ColumnHandle, Integer> channelIndexes)
    {
        try {
            return cache.get(new CacheKey(channelIndexes, dynamicFilter), () -> new DynamicPageFilter(
                    dynamicFilter,
                    channelIndexes,
                    typeOperators,
                    isolatedBlockFilterFactory,
                    executor));
        }
        catch (ExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw new RuntimeException(e.getCause());
        }
    }

    private record CacheKey(Map<ColumnHandle, Integer> channelIndexes, DynamicFilter dynamicFilter)
    {
        public CacheKey(Map<ColumnHandle, Integer> channelIndexes, DynamicFilter dynamicFilter)
        {
            this.channelIndexes = ImmutableMap.copyOf(requireNonNull(channelIndexes, "channelIndexes is null"));
            this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
        }
    }
}
