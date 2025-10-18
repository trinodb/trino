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
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.Session;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.server.ServerConfig;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.catalog.CatalogStore;
import io.trino.spi.catalog.CatalogStoreFactory;
import io.trino.spi.connector.ConnectorName;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.H2QueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.connector.FileCatalogStore.computeCatalogVersion;
import static io.trino.testing.QueryAssertions.assertQuery;
import static io.trino.testing.QueryAssertions.assertQueryFails;
import static io.trino.testing.QueryAssertions.assertUpdate;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSession;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public class TestDynamicCatalogs
{
    private static final String BROKEN_CATALOG = "broken_catalog";
    private static final CatalogName BROKEN_CATALOG_NAME = new CatalogName(BROKEN_CATALOG);
    private static final ConnectorName MEMORY_CONNECTOR_NAME = new ConnectorName("memory");

    @Test
    public void testNewHealthyCatalog()
            throws Exception
    {
        String catalogName = "new_catalog" + randomNameSuffix();
        Session session = testSession();
        QueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setWorkerCount(0)
                .build();
        queryRunner.installPlugin(new MemoryPlugin());
        queryRunner.createCatalog("healthy_catalog", "memory", ImmutableMap.of("memory.max-data-per-node", "128MB"));
        H2QueryRunner h2QueryRunner = new H2QueryRunner();

        assertQuery(queryRunner, session, "SHOW CATALOGS", h2QueryRunner, "VALUES 'healthy_catalog', 'system'", false, false);

        assertUpdate(queryRunner, session, "CREATE CATALOG %s USING memory WITH (\"memory.max-data-per-node\" = '128MB')".formatted(catalogName), OptionalLong.empty(), Optional.empty());
        assertQuery(queryRunner, session, "SHOW CATALOGS", h2QueryRunner, "VALUES 'healthy_catalog', '" + catalogName + "', 'system'", false, false);

        assertUpdate(queryRunner, session, "CREATE TABLE %s.default.test_table (age INT)".formatted(catalogName), OptionalLong.empty(), Optional.empty());
        assertUpdate(queryRunner, session, "INSERT INTO %s.default.test_table VALUES (10)".formatted(catalogName), OptionalLong.of(1), Optional.empty());
        assertQuery(queryRunner, session, "SELECT * FROM %s.default.test_table".formatted(catalogName), h2QueryRunner, "VALUES (10)", false, false);

        assertUpdate(queryRunner, session, "DROP CATALOG " + catalogName, OptionalLong.empty(), Optional.empty());
        assertQuery(queryRunner, session, "SHOW CATALOGS", h2QueryRunner, "VALUES 'healthy_catalog', 'system'", false, false);
    }

    @Test
    public void testNewUnhealthyCatalog()
            throws Exception
    {
        Session session = testSession();
        QueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setAdditionalModule(new TestCatalogStoreModule())
                .setCoordinatorProperties(ImmutableMap.of("catalog.store", "prepopulated_memory"))
                .setWorkerCount(0)
                .build();
        queryRunner.installPlugin(new MemoryPlugin());
        queryRunner.createCatalog("healthy_catalog", "memory", ImmutableMap.of("memory.max-data-per-node", "128MB"));
        H2QueryRunner h2QueryRunner = new H2QueryRunner();

        assertQuery(queryRunner, session, "SHOW CATALOGS", h2QueryRunner, "VALUES 'healthy_catalog', '" + BROKEN_CATALOG + "', 'system'", false, false);
        assertQueryFails(queryRunner, session, "CREATE TABLE %s.default.test_table (age INT)".formatted(BROKEN_CATALOG), ".*Catalog '%s' failed to initialize and is disabled.*".formatted(BROKEN_CATALOG));
        assertQueryFails(queryRunner, session, "SELECT * FROM %s.default.test_table".formatted(BROKEN_CATALOG), ".*Catalog '%s' failed to initialize and is disabled.*".formatted(BROKEN_CATALOG));
        assertQueryFails(queryRunner, session, "CREATE CATALOG %s USING memory WITH (\"memory.max-data-per-node\" = '128MB')".formatted(BROKEN_CATALOG), ".*Catalog '%s' already exists.*".formatted(BROKEN_CATALOG));

        assertUpdate(queryRunner, session, "DROP CATALOG " + BROKEN_CATALOG, OptionalLong.empty(), Optional.empty());
        assertQuery(queryRunner, session, "SHOW CATALOGS", h2QueryRunner, "VALUES 'healthy_catalog', 'system'", false, false);
    }

    public static class TestCatalogStoreModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            if (buildConfigObject(ServerConfig.class).isCoordinator()) {
                install(new PrepopulatedInMemoryCatalogStoreModule());
            }
        }
    }

    private static class PrepopulatedInMemoryCatalogStoreModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder) {}

        @Provides
        @Singleton
        public PrepopulatedInMemoryCatalogStoreFactory createDbCatalogStoreFactory(CatalogStoreManager catalogStoreManager)
        {
            PrepopulatedInMemoryCatalogStoreFactory factory = new PrepopulatedInMemoryCatalogStoreFactory();
            catalogStoreManager.addCatalogStoreFactory(factory);
            return factory;
        }
    }

    private static class PrepopulatedInMemoryCatalogStoreFactory
            implements CatalogStoreFactory
    {
        @Override
        public String getName()
        {
            return "prepopulated_memory";
        }

        @Override
        public CatalogStore create(Map<String, String> config)
        {
            return new PrepopulatedInMemoryCatalogStore();
        }
    }

    private static class PrepopulatedInMemoryCatalogStore
            extends InMemoryCatalogStore
    {
        @Override
        public Collection<StoredCatalog> getCatalogs()
        {
            Collection<StoredCatalog> catalogs = super.getCatalogs();
            List<StoredCatalog> catalogsCopy = new ArrayList<>(catalogs);
            catalogsCopy.add(new StoredCatalog()
            {
                @Override
                public CatalogName name()
                {
                    return new CatalogName("broken_catalog");
                }

                @Override
                public CatalogProperties loadProperties()
                {
                    ImmutableMap<String, String> properties = ImmutableMap.of("non_existing", "false");
                    return new CatalogProperties(
                            BROKEN_CATALOG_NAME,
                            computeCatalogVersion(BROKEN_CATALOG_NAME, MEMORY_CONNECTOR_NAME, properties),
                            MEMORY_CONNECTOR_NAME,
                            properties);
                }
            });
            return catalogsCopy;
        }
    }
}
