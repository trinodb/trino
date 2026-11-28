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
import io.airlift.configuration.secrets.SecretsResolver;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.cache.CacheManagerConfig;
import io.trino.cache.CacheManagerRegistry;
import io.trino.metadata.Catalog;
import io.trino.metadata.CatalogMetadata.SecurityManagement;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorName;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.tracing.Tracing.noopTracer;
import static io.trino.connector.CatalogHandle.createRootCatalogHandle;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestCoordinatorDynamicCatalogManager
{
    private static final CatalogName TEST_CATALOG = new CatalogName("test_catalog");

    @Test
    void testDropCatalogReturnsRemovedCatalog()
    {
        CoordinatorDynamicCatalogManager catalogManager = createCatalogManager(false);
        catalogManager.createCatalog(TEST_CATALOG, new ConnectorName("mock"), ImmutableMap.of(), false);
        Optional<Catalog> activeCatalog = catalogManager.getCatalog(TEST_CATALOG);
        assertThat(activeCatalog).isPresent();

        Optional<Catalog> droppedCatalog = catalogManager.dropCatalog(TEST_CATALOG, false);

        // the returned catalog has to be the exact instance that was unregistered, observed
        // atomically with the removal, so its state is the state the catalog was dropped in
        assertThat(droppedCatalog).isPresent();
        assertThat(droppedCatalog.get()).isSameAs(activeCatalog.get());
        assertThat(catalogManager.getCatalog(TEST_CATALOG)).isEmpty();
    }

    @Test
    void testDropCatalogReturnsSecurityManagementOfRemovedCatalog()
    {
        CoordinatorDynamicCatalogManager catalogManager = createCatalogManager(false);
        catalogManager.createCatalog(TEST_CATALOG, new ConnectorName("mock"), ImmutableMap.of(), false);
        assertThat(catalogManager.dropCatalog(TEST_CATALOG, false).flatMap(Catalog::getSecurityManagement))
                .hasValue(SecurityManagement.SYSTEM);

        CoordinatorDynamicCatalogManager connectorManaged = createCatalogManager(true);
        connectorManaged.createCatalog(TEST_CATALOG, new ConnectorName("mock"), ImmutableMap.of(), false);
        assertThat(connectorManaged.dropCatalog(TEST_CATALOG, false).flatMap(Catalog::getSecurityManagement))
                .hasValue(SecurityManagement.CONNECTOR);
    }

    @Test
    void testDropCatalogIfExistsOnMissingCatalogReturnsEmpty()
    {
        CoordinatorDynamicCatalogManager catalogManager = createCatalogManager(false);
        assertThat(catalogManager.dropCatalog(TEST_CATALOG, true)).isEmpty();
    }

    @Test
    void testDropCatalogOnMissingCatalogThrows()
    {
        CoordinatorDynamicCatalogManager catalogManager = createCatalogManager(false);
        assertThatThrownBy(() -> catalogManager.dropCatalog(TEST_CATALOG, false))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Catalog '%s' not found".formatted(TEST_CATALOG));
    }

    private static CoordinatorDynamicCatalogManager createCatalogManager(boolean connectorManaged)
    {
        CacheManagerRegistry cacheManagerRegistry = new CacheManagerRegistry(
                OpenTelemetry.noop(),
                noopTracer(),
                new SecretsResolver(ImmutableMap.of()),
                new CacheManagerConfig());
        return new CoordinatorDynamicCatalogManager(new InMemoryCatalogStore(), new MockCatalogFactory(connectorManaged), cacheManagerRegistry, directExecutor());
    }

    private static class MockCatalogFactory
            implements CatalogFactory
    {
        private final boolean connectorManaged;

        MockCatalogFactory(boolean connectorManaged)
        {
            this.connectorManaged = connectorManaged;
        }

        @Override
        public void addConnectorFactory(ConnectorFactory connectorFactory) {}

        @Override
        public CatalogConnector createCatalog(CatalogProperties catalogProperties)
        {
            MockConnectorFactory.Builder connectorFactory = MockConnectorFactory.builder();
            if (connectorManaged) {
                connectorFactory.withListTablePrivileges((_, _) -> List.of());
            }
            Connector connector = connectorFactory.build().create(catalogProperties.name().toString(), catalogProperties.properties(), new TestingConnectorContext());
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
}
