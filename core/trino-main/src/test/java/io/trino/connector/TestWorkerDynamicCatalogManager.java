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
package io.trino.connector;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.secrets.SecretsResolver;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.cache.CacheManagerConfig;
import io.trino.cache.CacheManagerRegistry;
import io.trino.spi.cache.Blob;
import io.trino.spi.cache.BlobCache;
import io.trino.spi.cache.BlobCacheManager;
import io.trino.spi.cache.BlobCacheManagerFactory;
import io.trino.spi.cache.BlobSource;
import io.trino.spi.cache.CacheCapability;
import io.trino.spi.cache.CacheKey;
import io.trino.spi.cache.CacheManagerContext;
import io.trino.spi.cache.NoopBlob;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorName;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.airlift.tracing.Tracing.noopTracer;
import static io.trino.connector.CatalogHandle.createRootCatalogHandle;
import static org.assertj.core.api.Assertions.assertThat;

class TestWorkerDynamicCatalogManager
{
    @Test
    void testPrunedCatalogCacheIsDropped()
    {
        RecordingBlobCacheManagerFactory cacheManagerFactory = new RecordingBlobCacheManagerFactory();
        CacheManagerRegistry registry = new CacheManagerRegistry(OpenTelemetry.noop(), noopTracer(), new SecretsResolver(ImmutableMap.of()), new CacheManagerConfig());
        registry.addBlobCacheManagerFactory(cacheManagerFactory);
        registry.loadBlobCacheManager("recording", Map.of());

        WorkerDynamicCatalogManager catalogManager = new WorkerDynamicCatalogManager(new MockCatalogFactory(), registry);

        CatalogProperties keptCatalog = catalogProperties("kept_catalog");
        CatalogProperties prunedCatalog = catalogProperties("pruned_catalog");
        catalogManager.ensureCatalogsLoaded(List.of(keptCatalog, prunedCatalog));

        // Prune everything not in use: only the kept catalog is
        catalogManager.pruneCatalogs(
                catalogManager.getPrunableState(),
                ImmutableSet.of(createRootCatalogHandle(keptCatalog.name(), keptCatalog.version())));

        assertThat(cacheManagerFactory.droppedCatalogs()).containsExactly(prunedCatalog.name());
    }

    private static CatalogProperties catalogProperties(String name)
    {
        return new CatalogProperties(new CatalogName(name), new CatalogVersion("1"), new ConnectorName("mock"), ImmutableMap.of());
    }

    private static class MockCatalogFactory
            implements CatalogFactory
    {
        @Override
        public void addConnectorFactory(ConnectorFactory connectorFactory) {}

        @Override
        public CatalogConnector createCatalog(CatalogProperties catalogProperties)
        {
            Connector connector = MockConnectorFactory.create().create(catalogProperties.name().toString(), catalogProperties.properties(), new TestingConnectorContext());
            CatalogHandle catalogHandle = createRootCatalogHandle(catalogProperties.name(), catalogProperties.version());
            ConnectorServices connectorServices = new ConnectorServices(noopTracer(), catalogHandle, connector);
            return new CatalogConnector(
                    catalogHandle,
                    new ConnectorName("mock"),
                    connectorServices,
                    connectorServices,
                    connectorServices,
                    Optional.of(catalogProperties));
        }

        @Override
        public CatalogConnector createCatalog(CatalogHandle catalogHandle, ConnectorName connectorName, Connector connector)
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class RecordingBlobCacheManagerFactory
            implements BlobCacheManagerFactory
    {
        private final List<CatalogName> droppedCatalogs = new ArrayList<>();

        public List<CatalogName> droppedCatalogs()
        {
            return droppedCatalogs;
        }

        @Override
        public String getName()
        {
            return "recording";
        }

        @Override
        public BlobCacheManager create(Map<String, String> config, CacheManagerContext context)
        {
            return new BlobCacheManager()
            {
                @Override
                public boolean hasCapability(CacheCapability capability)
                {
                    return true;
                }

                @Override
                public BlobCache create(CatalogName catalog, Set<CacheCapability> capabilities)
                {
                    return new BlobCache()
                    {
                        @Override
                        public Blob get(CacheKey key, BlobSource source)
                        {
                            return new NoopBlob(source);
                        }

                        @Override
                        public void tryInvalidate(CacheKey prefix) {}
                    };
                }

                @Override
                public void drop(CatalogName catalog)
                {
                    droppedCatalogs.add(catalog);
                }

                @Override
                public void shutdown() {}
            };
        }
    }
}
