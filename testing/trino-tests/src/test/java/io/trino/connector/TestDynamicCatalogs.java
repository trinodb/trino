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
import io.trino.metadata.CatalogManager;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.catalog.CatalogStore;
import io.trino.spi.connector.ConnectorName;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.H2QueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.testing.QueryAssertions.assertQuery;
import static io.trino.testing.QueryAssertions.assertQueryFails;
import static io.trino.testing.QueryAssertions.assertUpdate;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSession;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public class TestDynamicCatalogs
{
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
        ConnectorServicesProvider connectorServicesProvider = queryRunner.getCoordinator().getInstance(new Key<>() {});
        connectorServicesProvider.loadInitialCatalogs();
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
        String catalogName = "new_catalog" + randomNameSuffix();
        // simulate loading an unhealthy catalog during a startup
        Session session = testSession();
        QueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setWorkerCount(0)
                .build();
        queryRunner.installPlugin(new MemoryPlugin());
        queryRunner.createCatalog("healthy_catalog", "memory", ImmutableMap.of("memory.max-data-per-node", "128MB"));
        H2QueryRunner h2QueryRunner = new H2QueryRunner();
        CatalogStore catalogStore = queryRunner.getCoordinator().getInstance(new Key<>() {});
        ConnectorServicesProvider connectorServicesProvider = queryRunner.getCoordinator().getInstance(new Key<>() {});
        CatalogManager catalogManager = queryRunner.getCoordinator().getInstance(new Key<>() {});
        CatalogProperties catalogProperties = catalogStore.createCatalogProperties(new CatalogName(catalogName), new ConnectorName("memory"), ImmutableMap.of("invalid", "128MB"));
        catalogStore.addOrReplaceCatalog(catalogProperties);
        connectorServicesProvider.loadInitialCatalogs();

        assertQuery(queryRunner, session, "SHOW CATALOGS", h2QueryRunner, "VALUES 'healthy_catalog', '" + catalogName + "', 'system'", false, false);
        assertQueryFails(queryRunner, session, "CREATE TABLE %s.default.test_table (age INT)".formatted(catalogName), ".*Catalog '%s' failed to initialize and is disabled.*".formatted(catalogName));
        assertQueryFails(queryRunner, session, "CREATE CATALOG %s USING memory WITH (\"memory.max-data-per-node\" = '128MB')".formatted(catalogName), ".*Catalog '%s' already exists.*".formatted(catalogName));
        assertQueryFails(queryRunner, session, "DROP CATALOG " + catalogName, ".*Catalog '%s' failed to initialize and is disabled.*".formatted(catalogName));

        catalogManager.dropCatalog(new CatalogName(catalogName), false);
    }
}
