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
import com.google.inject.Key;
import io.trino.Session;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.catalog.CatalogStore;
import io.trino.spi.connector.ConnectorName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public class TestDynamicCatalogs
        extends AbstractTestQueryFramework
{
    private CatalogStore catalogStore;
    private ConnectorServicesProvider connectorServicesProvider;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder().build();
        QueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setWorkerCount(0)
                .build();
        queryRunner.installPlugin(new MemoryPlugin());
        queryRunner.createCatalog("healthy_catalog", "memory", ImmutableMap.of("memory.max-data-per-node", "128MB"));
        catalogStore = queryRunner.getCoordinator().getInstance(new Key<>() {});
        connectorServicesProvider = queryRunner.getCoordinator().getInstance(new Key<>() {});
        return queryRunner;
    }

    @Test
    public void testHealthyCatalog()
    {
        String catalogName = "new_catalog" + randomNameSuffix();
        assertQuery("SHOW CATALOGS", "VALUES 'healthy_catalog', 'system'");

        assertUpdate("CREATE CATALOG %s USING memory WITH (\"memory.max-data-per-node\" = '128MB')".formatted(catalogName));
        assertQuery("SHOW CATALOGS", "VALUES 'healthy_catalog', '" + catalogName + "', 'system'");

        assertUpdate("CREATE TABLE %s.default.test_table (age INT)".formatted(catalogName));
        assertUpdate("INSERT INTO %s.default.test_table VALUES (10)".formatted(catalogName), 1);
        assertQuery("SELECT * FROM %s.default.test_table".formatted(catalogName), "VALUES (10)");

        assertUpdate("DROP CATALOG " + catalogName);
        assertQuery("SHOW CATALOGS", "VALUES 'healthy_catalog', 'system'");
    }

    @Test
    public void testUnhealthyCatalog()
    {
        String catalogName = "new_catalog" + randomNameSuffix();
        // simulate loading an unhealthy catalog during a startup
        CatalogProperties catalogProperties = catalogStore.createCatalogProperties(new CatalogName(catalogName), new ConnectorName("memory"), ImmutableMap.of("invalid", "128MB"));
        catalogStore.addOrReplaceCatalog(catalogProperties);
        connectorServicesProvider.loadInitialCatalogs();

        assertQuery("SHOW CATALOGS", "VALUES 'healthy_catalog', '" + catalogName + "', 'system'");
        assertQueryFails("CREATE TABLE %s.default.test_table (age INT)".formatted(catalogName), ".*Catalog '%s' failed to initialize and is disabled.*".formatted(catalogName));
        assertQueryFails("CREATE CATALOG %s USING memory WITH (\"memory.max-data-per-node\" = '128MB')".formatted(catalogName), ".*Catalog '%s' already exists.*".formatted(catalogName));
        assertUpdate("DROP CATALOG " + catalogName);
    }
}
