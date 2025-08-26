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
package io.trino.connector.system;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Key;
import io.trino.Session;
import io.trino.connector.ConnectorServicesProvider;
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

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public class TestSystemMetadataCatalogTable
        extends AbstractTestQueryFramework
{
    private CatalogStore catalogStore;
    private ConnectorServicesProvider catalogManager;

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
        catalogManager = queryRunner.getCoordinator().getInstance(new Key<>() {});
        return queryRunner;
    }

    @Test
    public void testCatalogTableShowsOnlyLoadedCatalogs()
    {
        assertQuery("SELECT * FROM system.metadata.catalogs", "VALUES" +
                "('healthy_catalog', 'healthy_catalog', 'memory'), " +
                "('system', 'system', 'system')");

        assertUpdate("CREATE CATALOG brain USING memory WITH (\"memory.max-data-per-node\" = '128MB')");
        assertQuery("SELECT * FROM system.metadata.catalogs", "VALUES" +
                "('healthy_catalog', 'healthy_catalog', 'memory'), " +
                "('brain', 'brain', 'memory'), " +
                "('system', 'system', 'system')");
        CatalogProperties catalogProperties = catalogStore.createCatalogProperties(new CatalogName("broken"), new ConnectorName("memory"), ImmutableMap.of("memory.max-data-per-n", "128MB"));
        catalogStore.addOrReplaceCatalog(catalogProperties);
        catalogManager.loadInitialCatalogs();

        assertQuery("SELECT * FROM system.metadata.catalogs", "VALUES" +
                "('healthy_catalog', 'healthy_catalog', 'memory'), " +
                "('brain', 'brain', 'memory'), " +
                "('system', 'system', 'system')");
    }
}
