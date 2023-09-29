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
package io.trino.filesystem.alluxio;

import alluxio.conf.AlluxioConfiguration;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.cache.CacheKeyProvider;
import io.trino.filesystem.cache.TrinoFileSystemCache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

public class TestingAlluxioFileSystemCache
        implements TrinoFileSystemCache
{
    private final AlluxioFileSystemCache cache;

    @Override
    public TrinoInput cacheInput(TrinoInputFile delegate, String key)
            throws IOException
    {
        return cache.cacheInput(delegate, key);
    }

    @Override
    public TrinoInputStream cacheStream(TrinoInputFile delegate, String key)
            throws IOException
    {
        return cache.cacheStream(delegate, key);
    }

    @Override
    public void expire(Location location)
            throws IOException
    {
    }

    public enum OperationType
    {
        CACHE_READ,
        EXTERNAL_READ,
    }

    public record OperationContext(OperationType type, Location location)
    {
        public OperationContext
        {
            requireNonNull(type, "type is null");
            requireNonNull(location, "location is null");
        }
    }

    private final TestingAlluxioCacheStats statistics;
    private final CacheKeyProvider delegateKeyProvider;
    private final TestingCacheKeyProvider keyProvider;
    private final AtomicInteger cacheGeneration = new AtomicInteger(0);

    public TestingAlluxioFileSystemCache(AlluxioConfiguration alluxioConfiguration, CacheKeyProvider keyProvider)
    {
        statistics = new TestingAlluxioCacheStats();
        cache = new AlluxioFileSystemCache(AlluxioFileSystemCacheModule.getCacheManager(alluxioConfiguration), alluxioConfiguration, statistics);
        delegateKeyProvider = requireNonNull(keyProvider, "keyProvider is null");
        this.keyProvider = new TestingCacheKeyProvider();
    }

    public CacheKeyProvider getCacheKeyProvider()
    {
        return this.keyProvider;
    }

    private class TestingCacheKeyProvider
            implements CacheKeyProvider
    {
        @Override
        public Optional<String> getCacheKey(TrinoInputFile delegate)
                throws IOException
        {
            return delegateKeyProvider.getCacheKey(delegate).map(key -> key + cacheGeneration.get());
        }
    }

    public void reset()
    {
        statistics.reset();
    }

    public void clear()
    {
        cacheGeneration.incrementAndGet();
    }

    public Map<OperationContext, List<Integer>> getOperationCounts()
    {
        return statistics.getOperationCounts();
    }

    private static class TestingAlluxioCacheStats
            implements CacheStats
    {
        private final Map<OperationContext, List<Integer>> operationCounts = new ConcurrentHashMap<>();

        public TestingAlluxioCacheStats()
        {
            super();
        }

        @Override
        public void recordExternalRead(Location location, int length)
        {
            operationCounts.computeIfAbsent(new OperationContext(OperationType.EXTERNAL_READ, location), k -> new ArrayList<>()).add(length);
        }

        @Override
        public void recordCacheRead(Location location, int length)
        {
            operationCounts.computeIfAbsent(new OperationContext(OperationType.CACHE_READ, location), k -> new ArrayList<>()).add(length);
        }

        public Map<OperationContext, List<Integer>> getOperationCounts()
        {
            return ImmutableMap.copyOf(operationCounts);
        }

        public void reset()
        {
            operationCounts.clear();
        }
    }
}
