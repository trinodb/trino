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
package io.trino.faulttolerant;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MoreCollectors;
import io.trino.Session;
import io.trino.execution.QueryState;
import io.trino.plugin.blackhole.BlackHolePlugin;
import io.trino.plugin.exchange.filesystem.FileSystemExchangePlugin;
import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.server.BasicQueryInfo;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.FaultTolerantExecutionConnectorTestHelper;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMetadataOnlyQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        ImmutableMap<String, String> exchangeManagerProperties = ImmutableMap.<String, String>builder()
                .put("exchange.base-directories", System.getProperty("java.io.tmpdir") + "/trino-local-file-system-exchange-manager")
                .buildOrThrow();

        DistributedQueryRunner queryRunner = MemoryQueryRunner.builder()
                .setExtraProperties(FaultTolerantExecutionConnectorTestHelper.getExtraProperties())
                .setAdditionalSetup(runner -> {
                    runner.installPlugin(new FileSystemExchangePlugin());
                    runner.loadExchangeManager("filesystem", exchangeManagerProperties);
                })
                .setInitialTables(TpchTable.getTables())
                .build();

        try {
            queryRunner.installPlugin(new BlackHolePlugin());
            queryRunner.createCatalog("blackhole", "blackhole");
        }
        catch (RuntimeException e) {
            throw closeAllSuppress(e, queryRunner);
        }
        return queryRunner;
    }

    @Test
    @Timeout(120)
    public void testMetadataOnlyQueries()
            throws InterruptedException
    {
        // enforce single task uses whole node
        Session highTaskMemorySession = Session.builder(getSession())
                .setSystemProperty("fault_tolerant_execution_coordinator_task_memory", "500GB")
                .setSystemProperty("fault_tolerant_execution_task_memory", "500GB")
                // enforce each split in separate task
                .setSystemProperty("fault_tolerant_execution_arbitrary_distribution_compute_task_target_size_min", "1B")
                .setSystemProperty("fault_tolerant_execution_arbitrary_distribution_compute_task_target_size_max", "1B")
                .build();

        String slowTableName = "blackhole.default.testMetadataOnlyQueries_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + slowTableName + " (a INT, b INT) WITH (split_count = 3, pages_per_split = 1, rows_per_page = 1, page_processing_delay = '1d')");

        String slowQuery = "select count(*) FROM " + slowTableName;
        String nonMetadataQuery = "select count(*) non_metadata_query_count_" + System.currentTimeMillis() + " from nation";

        ExecutorService backgroundExecutor = newCachedThreadPool();
        try {
            backgroundExecutor.submit(() -> {
                assertUpdate(highTaskMemorySession, slowQuery);
            });
            assertEventually(() -> assertThat(queryState(slowQuery).orElseThrow()).isEqualTo(QueryState.RUNNING));

            assertThat(query("DESCRIBE lineitem")).succeeds();
            assertThat(query("SHOW TABLES")).succeeds();
            assertThat(query("SHOW TABLES LIKE 'line%'")).succeeds();
            assertThat(query("SHOW SCHEMAS")).succeeds();
            assertThat(query("SHOW SCHEMAS LIKE 'def%'")).succeeds();
            assertThat(query("SHOW CATALOGS")).succeeds();
            assertThat(query("SHOW CATALOGS LIKE 'mem%'")).succeeds();
            assertThat(query("SHOW FUNCTIONS")).succeeds();
            assertThat(query("SHOW FUNCTIONS LIKE 'split%'")).succeeds();
            assertThat(query("SHOW COLUMNS FROM lineitem")).succeeds();
            assertThat(query("SHOW SESSION")).succeeds();
            assertThat(query("SELECT count(*) FROM information_schema.tables")).succeeds();
            assertThat(query("SELECT * FROM system.jdbc.tables WHERE table_schem LIKE 'def%'")).succeeds();

            // check non-metadata queries still wait for resources
            backgroundExecutor.submit(() -> {
                assertUpdate(nonMetadataQuery);
            });
            assertEventually(() -> assertThat(queryState(nonMetadataQuery).orElseThrow()).isEqualTo(QueryState.STARTING));
            Thread.sleep(1000); // wait a bit longer and query should be still STARTING
            assertThat(queryState(nonMetadataQuery).orElseThrow()).isEqualTo(QueryState.STARTING);

            // slow query should be still running
            assertThat(queryState(slowQuery).orElseThrow()).isEqualTo(QueryState.RUNNING);
        }
        finally {
            cancelQuery(slowQuery);
            cancelQuery(nonMetadataQuery);
            backgroundExecutor.shutdownNow();
        }
    }

    private Optional<QueryState> queryState(String queryText)
    {
        return getDistributedQueryRunner().getCoordinator().getQueryManager().getQueries().stream()
                .filter(query -> query.getQuery().equals(queryText))
                .collect(MoreCollectors.toOptional())
                .map(BasicQueryInfo::getState);
    }

    private void cancelQuery(String queryText)
    {
        getDistributedQueryRunner().getCoordinator().getQueryManager().getQueries().stream()
                .filter(query -> query.getQuery().equals(queryText))
                .forEach(query -> {
                    try {
                        getDistributedQueryRunner().getCoordinator().getQueryManager().cancelQuery(query.getQueryId());
                    }
                    catch (Exception e) {
                        // ignore
                    }
                });
    }
}
