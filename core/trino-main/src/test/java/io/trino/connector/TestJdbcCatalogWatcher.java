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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import io.trino.metadata.Catalog;
import io.trino.metadata.CatalogManager;
import io.trino.connector.CatalogHandle;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.connector.ConnectorName;
import org.h2.jdbcx.JdbcDataSource;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class TestJdbcCatalogWatcher
{
    private static final CatalogName TEST_CATALOG = new CatalogName("test_catalog");
    private static final ConnectorName MEMORY_CONNECTOR = new ConnectorName("memory");

    private Jdbi jdbi;
    private JdbcCatalogStoreDao dao;
    private JdbcCatalogStore jdbcCatalogStore;
    private ScheduledExecutorService executorService;
    private RecordingCatalogManager catalogManager;

    @BeforeEach
    void setUp()
    {
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
        jdbcCatalogStore = new JdbcCatalogStore(dao, config);

        executorService = Executors.newSingleThreadScheduledExecutor();
        catalogManager = new RecordingCatalogManager();
    }

    @AfterEach
    void tearDown()
    {
        executorService.shutdownNow();
        if (jdbi != null) {
            jdbi.useHandle(handle -> handle.execute("DROP TABLE IF EXISTS catalogs"));
        }
    }

    @Test
    void testPollDetectsNewCatalog()
            throws InterruptedException
    {
        JdbcCatalogStoreConfig config = new JdbcCatalogStoreConfig()
                .setPollingEnabled(true)
                .setPollingInterval(new Duration(100, TimeUnit.MILLISECONDS));

        JdbcCatalogWatcher watcher = new JdbcCatalogWatcher(catalogManager, jdbcCatalogStore, config, executorService);
        watcher.start();

        // Initially empty
        assertThat(catalogManager.getCreatedCatalogs()).isEmpty();

        // Add catalog to DB
        jdbcCatalogStore.addOrReplaceCatalog(jdbcCatalogStore.createCatalogProperties(TEST_CATALOG, MEMORY_CONNECTOR, ImmutableMap.of()));

        // Wait for poll
        Thread.sleep(500);

        assertThat(catalogManager.getCreatedCatalogs()).contains(TEST_CATALOG);
    }

    @Test
    void testPollDetectsUpdate()
            throws InterruptedException
    {
        JdbcCatalogStoreConfig config = new JdbcCatalogStoreConfig()
                .setPollingEnabled(true)
                .setPollingInterval(new Duration(100, TimeUnit.MILLISECONDS));

        // Create initial catalog
        jdbcCatalogStore.addOrReplaceCatalog(jdbcCatalogStore.createCatalogProperties(TEST_CATALOG, MEMORY_CONNECTOR, ImmutableMap.of("v", "1")));

        JdbcCatalogWatcher watcher = new JdbcCatalogWatcher(catalogManager, jdbcCatalogStore, config, executorService);
        watcher.start();

        // Should be detected as "new" to the watcher on first run/poll or pre-existing?
        // Watcher initializes internal state on start().
        // So it knows v1 exists.

        Thread.sleep(200);
        catalogManager.clear(); // Clear initial load if any

        // Update catalog
        jdbcCatalogStore.addOrReplaceCatalog(jdbcCatalogStore.createCatalogProperties(TEST_CATALOG, MEMORY_CONNECTOR, ImmutableMap.of("v", "2")));

        Thread.sleep(500);

        assertThat(catalogManager.getDroppedCatalogs()).contains(TEST_CATALOG);
        assertThat(catalogManager.getCreatedCatalogs()).contains(TEST_CATALOG);
    }

    @Test
    void testPollDetectsDeletion()
            throws InterruptedException
    {
        JdbcCatalogStoreConfig config = new JdbcCatalogStoreConfig()
                .setPollingEnabled(true)
                .setPollingInterval(new Duration(100, TimeUnit.MILLISECONDS));

        jdbcCatalogStore.addOrReplaceCatalog(jdbcCatalogStore.createCatalogProperties(TEST_CATALOG, MEMORY_CONNECTOR, ImmutableMap.of()));

        JdbcCatalogWatcher watcher = new JdbcCatalogWatcher(catalogManager, jdbcCatalogStore, config, executorService);
        watcher.start();

        Thread.sleep(200);
        catalogManager.clear();

        // Delete
        jdbcCatalogStore.removeCatalog(TEST_CATALOG);

        Thread.sleep(500);

        assertThat(catalogManager.getDroppedCatalogs()).contains(TEST_CATALOG);
    }

    private static class RecordingCatalogManager
            implements CatalogManager
    {
        private final List<CatalogName> createdCatalogs = Collections.synchronizedList(new ArrayList<>());
        private final List<CatalogName> droppedCatalogs = Collections.synchronizedList(new ArrayList<>());

        public List<CatalogName> getCreatedCatalogs()
        {
            return ImmutableList.copyOf(createdCatalogs);
        }

        public List<CatalogName> getDroppedCatalogs()
        {
            return ImmutableList.copyOf(droppedCatalogs);
        }

        public void clear()
        {
            createdCatalogs.clear();
            droppedCatalogs.clear();
        }

        @Override
        public Set<CatalogName> getCatalogNames()
        {
            return ImmutableSet.copyOf(createdCatalogs);
        }

        @Override
        public Optional<Catalog> getCatalog(CatalogName catalogName)
        {
            return Optional.empty();
        }

        @Override
        public Optional<CatalogProperties> getCatalogProperties(CatalogHandle catalogHandle)
        {
            return Optional.empty();
        }

        @Override
        public void createCatalog(CatalogName catalogName, ConnectorName connectorName, Map<String, String> properties, boolean notExists)
        {
            createdCatalogs.add(catalogName);
        }

        @Override
        public Set<CatalogHandle> getActiveCatalogs()
        {
            return ImmutableSet.of();
        }

        @Override
        public void dropCatalog(CatalogName catalogName, boolean exists)
        {
            droppedCatalogs.add(catalogName);
        }
    }
}
