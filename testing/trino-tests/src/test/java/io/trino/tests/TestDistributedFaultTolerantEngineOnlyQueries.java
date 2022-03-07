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
package io.trino.tests;

import com.google.common.collect.ImmutableMap;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.plugin.exchange.FileSystemExchangePlugin;
import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.testing.AbstractDistributedEngineOnlyQueries;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.FaultTolerantExecutionConnectorTestHelper;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDistributedFaultTolerantEngineOnlyQueries
        extends AbstractDistributedEngineOnlyQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        ImmutableMap<String, String> exchangeManagerProperties = ImmutableMap.<String, String>builder()
                .put("exchange.base-directory", System.getProperty("java.io.tmpdir") + "/trino-local-file-system-exchange-manager")
                .buildOrThrow();

        DistributedQueryRunner queryRunner = MemoryQueryRunner.builder()
                .setExtraProperties(FaultTolerantExecutionConnectorTestHelper.getExtraProperties())
                .setAdditionalSetup(runner -> {
                    runner.installPlugin(new FileSystemExchangePlugin());
                    runner.loadExchangeManager("filesystem", exchangeManagerProperties);
                })
                .setInitialTables(TpchTable.getTables())
                .build();

        queryRunner.getCoordinator().getSessionPropertyManager().addSystemSessionProperties(TEST_SYSTEM_PROPERTIES);
        try {
            queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                    .withSessionProperties(TEST_CATALOG_PROPERTIES)
                    .build()));
            queryRunner.createCatalog(TESTING_CATALOG, "mock");
        }
        catch (RuntimeException e) {
            throw closeAllSuppress(e, queryRunner);
        }
        return queryRunner;
    }

    @Override
    public void testExplainAnalyzeDynamicFilterInfo()
    {
        // dynamic filters are disabled
        String result = (String) computeActual("EXPLAIN ANALYZE SELECT * FROM nation a, nation b WHERE a.nationkey = b.nationkey").getOnlyValue();
        assertThat(result).doesNotContainPattern("Dynamic filters:.*");
    }
}
