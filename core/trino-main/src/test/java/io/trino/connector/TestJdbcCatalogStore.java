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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
class TestJdbcCatalogStore {
    private static final CatalogName TEST_CATALOG = new CatalogName("test_catalog");
    private static final ConnectorName MEMORY_CONNECTOR = new ConnectorName("memory");

    private PostgreSQLContainer<?> postgres;
    private MySQLContainer<?> mysql;

    @BeforeAll
    void startContainers() {
        postgres = new PostgreSQLContainer<>("postgres:15-alpine");
        postgres.start();

        mysql = new MySQLContainer<>("mysql:8.0")
                .withCommand("--default-authentication-plugin=mysql_native_password");
        mysql.start();
    }

    @AfterAll
    void stopContainers() {
        if (postgres != null) {
            postgres.stop();
        }
        if (mysql != null) {
            mysql.stop();
        }
    }

    private Stream<JdbcCatalogStoreConfig> dataSources() {
        // H2
        String h2DbName = "test_" + UUID.randomUUID().toString().replace("-", "");
        JdbcCatalogStoreConfig h2Config = new JdbcCatalogStoreConfig()
                .setUrl("jdbc:h2:mem:" + h2DbName + ";DB_CLOSE_DELAY=-1")
                .setReadOnly(false);

        // Postgres
        JdbcCatalogStoreConfig postgresConfig = new JdbcCatalogStoreConfig()
                .setUrl(postgres.getJdbcUrl())
                .setUser(postgres.getUsername())
                .setPassword(postgres.getPassword())
                .setReadOnly(false);

        // MySQL
        JdbcCatalogStoreConfig mysqlConfig = new JdbcCatalogStoreConfig()
                .setUrl(mysql.getJdbcUrl())
                .setUser(mysql.getUsername())
                .setPassword(mysql.getPassword())
                .setReadOnly(false);

        return Stream.of(h2Config, postgresConfig, mysqlConfig);
    }

    @ParameterizedTest
    @MethodSource("dataSources")
    void testAddCatalog(JdbcCatalogStoreConfig config) {
        Jdbi jdbi = Jdbi.create(config.getUrl(), config.getUser(), config.getPassword());
        jdbi.installPlugin(new SqlObjectPlugin());
        JdbcCatalogStoreDao dao = jdbi.onDemand(JdbcCatalogStoreDao.class);
        
        // Clean up before test (since containers are reused)
        try {
            dao.createCatalogsTable(); // Ensure exists
            jdbi.useHandle(handle -> handle.execute("DELETE FROM catalogs"));
        } catch (Exception ignored) {}

        JdbcCatalogStore catalogStore = new JdbcCatalogStore(dao, config);

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

    @ParameterizedTest
    @MethodSource("dataSources")
    void testReplaceCatalog(JdbcCatalogStoreConfig config) {
        Jdbi jdbi = Jdbi.create(config.getUrl(), config.getUser(), config.getPassword());
        jdbi.installPlugin(new SqlObjectPlugin());
        JdbcCatalogStoreDao dao = jdbi.onDemand(JdbcCatalogStoreDao.class);
        try {
            dao.createCatalogsTable();
            jdbi.useHandle(handle -> handle.execute("DELETE FROM catalogs"));
        } catch (Exception ignored) {}
        
        JdbcCatalogStore catalogStore = new JdbcCatalogStore(dao, config);

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

    @ParameterizedTest
    @MethodSource("dataSources")
    void testRemoveCatalog(JdbcCatalogStoreConfig config) {
        Jdbi jdbi = Jdbi.create(config.getUrl(), config.getUser(), config.getPassword());
        jdbi.installPlugin(new SqlObjectPlugin());
        JdbcCatalogStoreDao dao = jdbi.onDemand(JdbcCatalogStoreDao.class);
        try {
            dao.createCatalogsTable();
            jdbi.useHandle(handle -> handle.execute("DELETE FROM catalogs"));
        } catch (Exception ignored) {}

        JdbcCatalogStore catalogStore = new JdbcCatalogStore(dao, config);

        Map<String, String> properties = ImmutableMap.of("memory.max-data-per-node", "128MB");
        CatalogProperties catalogProperties = catalogStore.createCatalogProperties(TEST_CATALOG, MEMORY_CONNECTOR,
                properties);
        catalogStore.addOrReplaceCatalog(catalogProperties);

        assertThat(catalogStore.getCatalogs()).hasSize(1);

        catalogStore.removeCatalog(TEST_CATALOG);

        assertThat(catalogStore.getCatalogs()).isEmpty();
    }
}
