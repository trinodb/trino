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
package io.trino.metadata;

import io.trino.Session;
import io.trino.connector.CatalogConnector;
import io.trino.connector.CatalogHandle;
import io.trino.connector.ConnectorServices;
import io.trino.connector.MockConnectorFactory;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.connector.Connector;
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
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMetadataManagerDropCatalog
{
    private static final CatalogName TEST_CATALOG = new CatalogName("test_catalog");
    private static final Session SESSION = testSessionBuilder().build();

    @Test
    public void testCatalogDroppedCalledForSystemManagedDroppedCatalog()
    {
        // models the race from https://github.com/trinodb/trino/issues/26927:
        // a connector-managed catalog is observed by the pre-drop read, but a system-managed
        // replacement with the same name is what the drop actually removes (DROP + CREATE
        // sneaking in between). The replacement ceased to exist, so catalogDropped must be
        // called for it, even though the read view said the catalog was connector managed.
        RecordingSystemSecurityMetadata securityMetadata = new RecordingSystemSecurityMetadata();
        MetadataManager metadataManager = TestingMetadataManager.builder()
                .withCatalogManager(new FakeCatalogManager(
                        Optional.of(createCatalog(true)),
                        Optional.of(createCatalog(false))))
                .withSystemSecurityMetadata(securityMetadata)
                .build();

        metadataManager.dropCatalog(SESSION, TEST_CATALOG, false);

        assertThat(securityMetadata.droppedCatalogs).containsExactly(TEST_CATALOG);
    }

    @Test
    public void testCatalogDroppedNotCalledForConnectorManagedDroppedCatalog()
    {
        // same race the other way around: the read observes a system-managed catalog, but the
        // drop removes a connector-managed replacement. Reporting a catalogDropped for it would
        // leak a stale notification for a catalog that never used the system security metadata.
        RecordingSystemSecurityMetadata securityMetadata = new RecordingSystemSecurityMetadata();
        MetadataManager metadataManager = TestingMetadataManager.builder()
                .withCatalogManager(new FakeCatalogManager(
                        Optional.of(createCatalog(false)),
                        Optional.of(createCatalog(true))))
                .withSystemSecurityMetadata(securityMetadata)
                .build();

        metadataManager.dropCatalog(SESSION, TEST_CATALOG, false);

        assertThat(securityMetadata.droppedCatalogs).isEmpty();
    }

    @Test
    public void testCatalogDroppedCalledWhenReadAndDroppedCatalogAgree()
    {
        RecordingSystemSecurityMetadata securityMetadata = new RecordingSystemSecurityMetadata();
        MetadataManager metadataManager = TestingMetadataManager.builder()
                .withCatalogManager(new FakeCatalogManager(
                        Optional.of(createCatalog(false)),
                        Optional.of(createCatalog(false))))
                .withSystemSecurityMetadata(securityMetadata)
                .build();

        metadataManager.dropCatalog(SESSION, TEST_CATALOG, false);

        assertThat(securityMetadata.droppedCatalogs).containsExactly(TEST_CATALOG);
    }

    @Test
    public void testCatalogDroppedNotCalledWhenNothingWasDropped()
    {
        RecordingSystemSecurityMetadata securityMetadata = new RecordingSystemSecurityMetadata();
        MetadataManager metadataManager = TestingMetadataManager.builder()
                .withCatalogManager(new FakeCatalogManager(Optional.empty(), Optional.empty()))
                .withSystemSecurityMetadata(securityMetadata)
                .build();

        metadataManager.dropCatalog(SESSION, TEST_CATALOG, true);

        assertThat(securityMetadata.droppedCatalogs).isEmpty();
    }

    private static Catalog createCatalog(boolean connectorManaged)
    {
        MockConnectorFactory.Builder connectorFactory = MockConnectorFactory.builder();
        if (connectorManaged) {
            connectorFactory.withListTablePrivileges((_, _) -> List.of());
        }
        Connector connector = connectorFactory.build().create(TEST_CATALOG.toString(), Map.of(), new TestingConnectorContext());
        CatalogHandle catalogHandle = createRootCatalogHandle(TEST_CATALOG, new CatalogVersion("1"));
        ConnectorServices connectorServices = new ConnectorServices(noopTracer(), catalogHandle, connector);
        CatalogProperties catalogProperties = new CatalogProperties(TEST_CATALOG, new CatalogVersion("1"), new ConnectorName("mock"), Map.of());
        return new CatalogConnector(
                catalogHandle,
                new ConnectorName("mock"),
                connectorServices,
                connectorServices,
                connectorServices,
                Optional.of(catalogProperties))
                .getCatalog();
    }

    private static class FakeCatalogManager
            implements CatalogManager
    {
        private final Optional<Catalog> readCatalog;
        private final Optional<Catalog> droppedCatalog;

        public FakeCatalogManager(Optional<Catalog> readCatalog, Optional<Catalog> droppedCatalog)
        {
            this.readCatalog = requireNonNull(readCatalog, "readCatalog is null");
            this.droppedCatalog = requireNonNull(droppedCatalog, "droppedCatalog is null");
        }

        @Override
        public Set<CatalogName> getCatalogNames()
        {
            return readCatalog.map(catalog -> Set.of(catalog.getCatalogName())).orElse(Set.of());
        }

        @Override
        public Optional<Catalog> getCatalog(CatalogName catalogName)
        {
            return readCatalog;
        }

        @Override
        public Optional<CatalogProperties> getCatalogProperties(CatalogHandle catalogHandle)
        {
            return Optional.empty();
        }

        @Override
        public Set<CatalogHandle> getReachableDynamicCatalogs()
        {
            return Set.of();
        }

        @Override
        public void createCatalog(CatalogName catalogName, ConnectorName connectorName, Map<String, String> properties, boolean notExists)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<Catalog> dropCatalog(CatalogName catalogName, boolean exists)
        {
            return droppedCatalog;
        }
    }

    private static class RecordingSystemSecurityMetadata
            extends DisabledSystemSecurityMetadata
    {
        private final List<CatalogName> droppedCatalogs = new ArrayList<>();

        @Override
        public void catalogDropped(Session session, CatalogName catalog)
        {
            droppedCatalogs.add(catalog);
        }
    }
}
