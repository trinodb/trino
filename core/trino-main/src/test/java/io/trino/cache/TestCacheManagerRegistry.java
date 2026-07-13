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
package io.trino.cache;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.secrets.SecretsResolver;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.spi.cache.Blob;
import io.trino.spi.cache.BlobCache;
import io.trino.spi.cache.BlobCacheManager;
import io.trino.spi.cache.BlobCacheManagerFactory;
import io.trino.spi.cache.BlobSource;
import io.trino.spi.cache.CacheCapability;
import io.trino.spi.cache.CacheKey;
import io.trino.spi.cache.CacheManagerContext;
import io.trino.spi.cache.CacheRequirements;
import io.trino.spi.cache.ConnectorCacheFactory;
import io.trino.spi.cache.NoopBlob;
import io.trino.spi.catalog.CatalogName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.airlift.tracing.Tracing.noopTracer;
import static io.trino.spi.cache.CacheCapability.CAN_EXCEED_HEAP_SIZE;
import static io.trino.spi.cache.CacheCapability.LOW_LATENCY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestCacheManagerRegistry
{
    private static final CatalogName CATALOG = new CatalogName("example");

    @Test
    void testNoManagerLoaded()
    {
        CacheManagerRegistry registry = createRegistry();
        ConnectorCacheFactory cacheFactory = registry.createConnectorCacheFactory(CATALOG);

        assertThat(cacheFactory.createBlobCache(new CacheRequirements("testing.data", Set.of(LOW_LATENCY)))).isEmpty();
    }

    @Test
    void testNoQualifyingManager()
    {
        CacheManagerRegistry registry = createRegistry();
        registry.addBlobCacheManagerFactory(new TestingBlobCacheManagerFactory("memory", LOW_LATENCY));
        registry.loadBlobCacheManager("memory", Map.of());
        ConnectorCacheFactory cacheFactory = registry.createConnectorCacheFactory(CATALOG);

        assertThat(cacheFactory.createBlobCache(new CacheRequirements("testing.data", Set.of(CAN_EXCEED_HEAP_SIZE)))).isEmpty();
    }

    @Test
    void testSingleQualifyingManager()
            throws Exception
    {
        CacheManagerRegistry registry = createRegistry();
        TestingBlobCacheManagerFactory memory = new TestingBlobCacheManagerFactory("memory", LOW_LATENCY);
        TestingBlobCacheManagerFactory disk = new TestingBlobCacheManagerFactory("disk", CAN_EXCEED_HEAP_SIZE);
        registry.addBlobCacheManagerFactory(memory);
        registry.addBlobCacheManagerFactory(disk);
        registry.loadBlobCacheManager("memory", Map.of());
        registry.loadBlobCacheManager("disk", Map.of());
        ConnectorCacheFactory cacheFactory = registry.createConnectorCacheFactory(CATALOG);

        BlobCache cache = cacheFactory.createBlobCache(new CacheRequirements("testing.data", Set.of(CAN_EXCEED_HEAP_SIZE))).orElseThrow();
        cache.get(CacheKey.of("file", "version"), new TestingBlobSource()).close();

        assertThat(memory.recordedKeys()).isEmpty();
        assertThat(disk.recordedKeys()).containsExactly(new CacheKey(List.of("example", "testing.data", "file", "version")));
    }

    @Test
    void testEmptyRequirementsQualifyEveryManager()
            throws Exception
    {
        CacheManagerRegistry registry = createRegistry();
        TestingBlobCacheManagerFactory memory = new TestingBlobCacheManagerFactory("memory", LOW_LATENCY);
        registry.addBlobCacheManagerFactory(memory);
        registry.loadBlobCacheManager("memory", Map.of());
        ConnectorCacheFactory cacheFactory = registry.createConnectorCacheFactory(CATALOG);

        BlobCache cache = cacheFactory.createBlobCache(new CacheRequirements("testing.data", Set.of())).orElseThrow();
        cache.get(CacheKey.of("file"), new TestingBlobSource()).close();

        assertThat(memory.recordedKeys()).containsExactly(new CacheKey(List.of("example", "testing.data", "file")));
    }

    @Test
    void testMultipleQualifyingManagers()
    {
        CacheManagerRegistry registry = createRegistry();
        registry.addBlobCacheManagerFactory(new TestingBlobCacheManagerFactory("first", LOW_LATENCY));
        registry.addBlobCacheManagerFactory(new TestingBlobCacheManagerFactory("second", LOW_LATENCY));
        registry.loadBlobCacheManager("first", Map.of());
        registry.loadBlobCacheManager("second", Map.of());
        ConnectorCacheFactory cacheFactory = registry.createConnectorCacheFactory(CATALOG);

        assertThatThrownBy(() -> cacheFactory.createBlobCache(new CacheRequirements("testing.data", Set.of(LOW_LATENCY))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("all provide")
                .hasMessageContaining("testing.data");
    }

    @Test
    void testInvalidUsage()
    {
        CacheManagerRegistry registry = createRegistry();
        ConnectorCacheFactory cacheFactory = registry.createConnectorCacheFactory(CATALOG);

        assertThatThrownBy(() -> cacheFactory.createBlobCache(new CacheRequirements("", Set.of())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("usageName is null or empty");
    }

    @Test
    void testMemoryManagerLoadedByDefault()
    {
        CacheManagerRegistry registry = createRegistry();
        registry.addBlobCacheManagerFactory(new TestingBlobCacheManagerFactory("memory", LOW_LATENCY));
        registry.loadCacheManagers();
        ConnectorCacheFactory cacheFactory = registry.createConnectorCacheFactory(CATALOG);

        assertThat(cacheFactory.createBlobCache(new CacheRequirements("testing.metadata", Set.of(LOW_LATENCY)))).isPresent();
    }

    @Test
    void testNoDefaultWithoutMemoryFactory()
    {
        CacheManagerRegistry registry = createRegistry();
        registry.loadCacheManagers();
        ConnectorCacheFactory cacheFactory = registry.createConnectorCacheFactory(CATALOG);

        assertThat(cacheFactory.createBlobCache(new CacheRequirements("testing.metadata", Set.of(LOW_LATENCY)))).isEmpty();
    }

    @Test
    void testConflictingUsageRequirements()
    {
        CacheManagerRegistry registry = createRegistry();
        registry.addBlobCacheManagerFactory(new TestingBlobCacheManagerFactory("memory", LOW_LATENCY));
        registry.addBlobCacheManagerFactory(new TestingBlobCacheManagerFactory("disk", CAN_EXCEED_HEAP_SIZE));
        registry.loadBlobCacheManager("memory", Map.of());
        registry.loadBlobCacheManager("disk", Map.of());
        ConnectorCacheFactory cacheFactory = registry.createConnectorCacheFactory(CATALOG);

        assertThat(cacheFactory.createBlobCache(new CacheRequirements("testing.data", Set.of(LOW_LATENCY)))).isPresent();
        // identical repeated requests are idempotent
        assertThat(cacheFactory.createBlobCache(new CacheRequirements("testing.data", Set.of(LOW_LATENCY)))).isPresent();
        // reusing the usage name with different requirements is rejected
        assertThatThrownBy(() -> cacheFactory.createBlobCache(new CacheRequirements("testing.data", Set.of(CAN_EXCEED_HEAP_SIZE))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already issued");
        // dropping the catalog frees its usage names
        registry.drop(CATALOG);
        assertThat(cacheFactory.createBlobCache(new CacheRequirements("testing.data", Set.of(CAN_EXCEED_HEAP_SIZE)))).isPresent();
    }

    @Test
    void testManagerAlreadyLoaded()
    {
        CacheManagerRegistry registry = createRegistry();
        registry.addBlobCacheManagerFactory(new TestingBlobCacheManagerFactory("memory", LOW_LATENCY));
        registry.loadBlobCacheManager("memory", Map.of());

        assertThatThrownBy(() -> registry.loadBlobCacheManager("memory", Map.of()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already loaded");
    }

    private static CacheManagerRegistry createRegistry()
    {
        return new CacheManagerRegistry(OpenTelemetry.noop(), noopTracer(), new SecretsResolver(ImmutableMap.of()), new CacheManagerConfig());
    }

    private static class TestingBlobCacheManagerFactory
            implements BlobCacheManagerFactory
    {
        private final String name;
        private final Set<CacheCapability> capabilities;
        private final List<CacheKey> recordedKeys = new ArrayList<>();

        public TestingBlobCacheManagerFactory(String name, CacheCapability... capabilities)
        {
            this.name = name;
            this.capabilities = ImmutableSet.copyOf(capabilities);
        }

        public List<CacheKey> recordedKeys()
        {
            return recordedKeys;
        }

        @Override
        public String getName()
        {
            return name;
        }

        @Override
        public BlobCacheManager create(Map<String, String> config, CacheManagerContext context)
        {
            return new BlobCacheManager()
            {
                @Override
                public boolean hasCapability(CacheCapability capability)
                {
                    return capabilities.contains(capability);
                }

                @Override
                public BlobCache create(CatalogName catalog, Set<CacheCapability> requested)
                {
                    return new BlobCache()
                    {
                        @Override
                        public Blob get(CacheKey key, BlobSource source)
                        {
                            recordedKeys.add(key);
                            return new NoopBlob(source);
                        }

                        @Override
                        public void tryInvalidate(CacheKey prefix)
                        {
                            recordedKeys.add(prefix);
                        }
                    };
                }

                @Override
                public void drop(CatalogName catalog) {}

                @Override
                public void shutdown() {}
            };
        }
    }

    private static class TestingBlobSource
            implements BlobSource
    {
        @Override
        public long length()
        {
            return 0;
        }

        @Override
        public void readFully(long position, byte[] buffer, int offset, int length) {}
    }
}
