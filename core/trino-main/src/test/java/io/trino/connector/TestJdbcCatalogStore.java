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
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.catalog.CatalogStore.StoredCatalog;
import io.trino.spi.connector.ConnectorName;
import org.h2.jdbcx.JdbcDataSource;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestJdbcCatalogStore {
    private static final CatalogName TEST_CATALOG = new CatalogName("test_catalog");
    private static final ConnectorName MEMORY_CONNECTOR = new ConnectorName("memory");

    private Jdbi jdbi;
    private JdbcCatalogStoreDao dao;
    private JdbcCatalogStore catalogStore;

    @BeforeEach
    void setUp() {
        // Use unique database name per test to ensure isolation
        String dbName = "test_" + UUID.randomUUID().toString().replace("-", "");
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:" + dbName + ";DB_CLOSE_DELAY=-1");

        jdbi = Jdbi.create(dataSource);
        jdbi.installPlugin(new SqlObjectPlugin());
        dao = jdbi.onDemand(JdbcCatalogStoreDao.class);
        dao.createCatalogsTable();

        JdbcCatalogStoreConfig config = new JdbcCatalogStoreConfig()
                .setUrl("jdbc:h2:mem:" + dbName)
                .setReadOnly(false);
        catalogStore = new JdbcCatalogStore(dao, config);
    }

    @AfterEach
    void tearDown() {
        if (jdbi != null) {
            jdbi.useHandle(handle -> handle.execute("DROP TABLE IF EXISTS catalogs"));
        }
    }

    @Test
    void testGetCatalogsEmpty() {
        Collection<StoredCatalog> catalogs = catalogStore.getCatalogs();
        assertThat(catalogs).isEmpty();
    }

    @Test
    void testAddCatalog() {
        Map<String, String> properties = ImmutableMap.of("memory.max-data-per-node", "128MB");
        CatalogProperties catalogProperties = catalogStore.createCatalogProperties(TEST_CATALOG, MEMORY_CONNECTOR,
                properties);

        catalogStore.addOrReplaceCatalog(catalogProperties);

        Collection<StoredCatalog> catalogs = catalogStore.getCatalogs();
        assertThat(catalogs).hasSize(1);

        StoredCatalog storedCatalog = catalogs.iterator().next();
        assertThat(storedCatalog.name()).isEqualTo(TEST_CATALOG);

        CatalogProperties loaded = storedCatalog.loadProperties();
        assertThat(loaded.name()).isEqualTo(TEST_CATALOG);
        assertThat(loaded.connectorName()).isEqualTo(MEMORY_CONNECTOR);
        assertThat(loaded.properties()).isEqualTo(properties);
    }

    @Test
    void testReplaceCatalog() {
        Map<String, String> initialProperties = ImmutableMap.of("memory.max-data-per-node", "128MB");
        CatalogProperties initialCatalog = catalogStore.createCatalogProperties(TEST_CATALOG, MEMORY_CONNECTOR,
                initialProperties);
        catalogStore.addOrReplaceCatalog(initialCatalog);

        Map<String, String> updatedProperties = ImmutableMap.of("memory.max-data-per-node", "256MB");
        CatalogProperties updatedCatalog = catalogStore.createCatalogProperties(TEST_CATALOG, MEMORY_CONNECTOR,
                updatedProperties);
        catalogStore.addOrReplaceCatalog(updatedCatalog);

        Collection<StoredCatalog> catalogs = catalogStore.getCatalogs();
        assertThat(catalogs).hasSize(1);

        CatalogProperties loaded = catalogs.iterator().next().loadProperties();
        assertThat(loaded.properties()).isEqualTo(updatedProperties);
    }

    @Test
    void testRemoveCatalog() {
        Map<String, String> properties = ImmutableMap.of("memory.max-data-per-node", "128MB");
        CatalogProperties catalogProperties = catalogStore.createCatalogProperties(TEST_CATALOG, MEMORY_CONNECTOR,
                properties);
        catalogStore.addOrReplaceCatalog(catalogProperties);

        assertThat(catalogStore.getCatalogs()).hasSize(1);

        catalogStore.removeCatalog(TEST_CATALOG);

        assertThat(catalogStore.getCatalogs()).isEmpty();
    }

    @Test
    void testRemoveNonExistentCatalog() {
        // Should not throw exception
        catalogStore.removeCatalog(new CatalogName("non_existent"));
    }

    @Test
    void testReadOnlyModeBlocksWrites() {
        JdbcCatalogStoreConfig readOnlyConfig = new JdbcCatalogStoreConfig()
                .setUrl("jdbc:h2:mem:test_catalog_store")
                .setReadOnly(true);
        JdbcCatalogStore readOnlyCatalogStore = new JdbcCatalogStore(dao, readOnlyConfig);

        Map<String, String> properties = ImmutableMap.of("memory.max-data-per-node", "128MB");

        assertThatThrownBy(
                () -> readOnlyCatalogStore.createCatalogProperties(TEST_CATALOG, MEMORY_CONNECTOR, properties))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("read only");

        assertThatThrownBy(() -> readOnlyCatalogStore.removeCatalog(TEST_CATALOG))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("read only");
    }

    @Test
    void testMultipleCatalogs() {
        CatalogName catalog1 = new CatalogName("catalog_one");
        CatalogName catalog2 = new CatalogName("catalog_two");

        catalogStore.addOrReplaceCatalog(catalogStore.createCatalogProperties(
                catalog1, MEMORY_CONNECTOR, ImmutableMap.of("prop1", "value1")));
        catalogStore.addOrReplaceCatalog(catalogStore.createCatalogProperties(
                catalog2, new ConnectorName("tpch"), ImmutableMap.of("prop2", "value2")));

        Collection<StoredCatalog> catalogs = catalogStore.getCatalogs();
        assertThat(catalogs).hasSize(2);
        assertThat(catalogs.stream().map(StoredCatalog::name))
                .containsExactlyInAnyOrder(catalog1, catalog2);
    }
}
