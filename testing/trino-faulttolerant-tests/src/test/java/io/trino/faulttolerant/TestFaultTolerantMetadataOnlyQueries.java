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
import io.trino.operator.RetryPolicy;
import io.trino.plugin.blackhole.BlackHolePlugin;
import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.server.BasicQueryInfo;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.FaultTolerantExecutionConnectorTestHelper;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.assertions.Assert.assertEventually;
import static io.trino.tpch.TpchTable.NATION;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;

public class TestFaultTolerantMetadataOnlyQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return MemoryQueryRunner.builder()
                .setExtraProperties(FaultTolerantExecutionConnectorTestHelper.getExtraProperties())
                .withExchange("filesystem")
                .setInitialTables(List.of(NATION))
                .build();
    }

    @Test
    @Timeout(120)
    public void testMetadataOnlyQueriesSucceedUnderResourcePressureWhenExclusionDisabled()
            throws Exception
    {
        // With exclusion disabled, metadata queries must still bypass FTE resource constraints
        // via the existing memory-budget mechanism (not via retry-policy downgrade)
        Map<String, String> extraProperties = ImmutableMap.<String, String>builder()
                .putAll(FaultTolerantExecutionConnectorTestHelper.getExtraProperties())
                .put("retry-policy.exclude-metadata-only-queries", "false")
                .buildOrThrow();
        try (DistributedQueryRunner queryRunner = MemoryQueryRunner.builder()
                .setExtraProperties(extraProperties)
                .withExchange("filesystem")
                .setInitialTables(List.of(NATION))
                .build()) {
            queryRunner.installPlugin(new BlackHolePlugin());
            queryRunner.createCatalog("blackhole", "blackhole");

            // enforce single task uses whole node
            Session highTaskMemorySession = Session.builder(queryRunner.getDefaultSession())
                    .setSystemProperty("fault_tolerant_execution_coordinator_task_memory", "500GB")
                    .setSystemProperty("fault_tolerant_execution_task_memory", "500GB")
                    // enforce each split in separate task
                    .setSystemProperty("fault_tolerant_execution_arbitrary_distribution_compute_task_target_size_min", "1B")
                    .setSystemProperty("fault_tolerant_execution_arbitrary_distribution_compute_task_target_size_max", "1B")
                    .build();

            String slowTableName = "blackhole.default.testMetadataOnlyQueries_" + randomNameSuffix();
            queryRunner.execute("CREATE TABLE " + slowTableName + " (a INT, b INT) WITH (split_count = 3, pages_per_split = 1, rows_per_page = 1, page_processing_delay = '1d')");

            String slowQuery = "select count(*) FROM " + slowTableName;
            String nonMetadataQuery = "select count(*) non_metadata_query_count_" + System.currentTimeMillis() + " from nation";

            ExecutorService backgroundExecutor = newCachedThreadPool();
            try {
                backgroundExecutor.submit(() -> queryRunner.execute(highTaskMemorySession, slowQuery));
                assertEventually(() -> assertThat(queryState(queryRunner, slowQuery).orElseThrow()).isEqualTo(QueryState.RUNNING));

                queryRunner.execute("DESCRIBE nation");
                queryRunner.execute("SHOW TABLES");
                queryRunner.execute("SHOW TABLES LIKE 'nat%'");
                queryRunner.execute("SHOW SCHEMAS");
                queryRunner.execute("SHOW SCHEMAS LIKE 'def%'");
                queryRunner.execute("SHOW CATALOGS");
                queryRunner.execute("SHOW CATALOGS LIKE 'mem%'");
                queryRunner.execute("SHOW FUNCTIONS");
                queryRunner.execute("SHOW FUNCTIONS LIKE 'split%'");
                queryRunner.execute("SHOW COLUMNS FROM nation");
                queryRunner.execute("SHOW SESSION");
                queryRunner.execute("SELECT count(*) FROM information_schema.tables");
                queryRunner.execute("SELECT * FROM system.jdbc.tables WHERE table_schem LIKE 'def%'");

                // check non-metadata queries still wait for resources
                backgroundExecutor.submit(() -> queryRunner.execute(nonMetadataQuery));
                assertEventually(() -> assertThat(queryState(queryRunner, nonMetadataQuery).orElseThrow()).isEqualTo(QueryState.STARTING));
                Thread.sleep(1000); // wait a bit longer and query should be still STARTING
                assertThat(queryState(queryRunner, nonMetadataQuery).orElseThrow()).isEqualTo(QueryState.STARTING);

                // slow query should be still running
                assertThat(queryState(queryRunner, slowQuery).orElseThrow()).isEqualTo(QueryState.RUNNING);
            }
            finally {
                cancelQuery(queryRunner, slowQuery);
                cancelQuery(queryRunner, nonMetadataQuery);
                backgroundExecutor.shutdownNow();
            }
        }
    }

    @Test
    @Timeout(120)
    public void testMetadataOnlyQueriesSkipFaultTolerantExecution()
    {
        List<String> metadataQueries = List.of(
                "SELECT * FROM information_schema.tables LIMIT 1",
                "SELECT * FROM system.jdbc.tables LIMIT 1");
        for (String sql : metadataQueries) {
            getQueryRunner().execute(sql);
        }

        getDistributedQueryRunner().getCoordinator().getQueryManager().getQueries().stream()
                .filter(query -> metadataQueries.contains(query.getQuery()))
                .forEach(query ->
                        assertThat(query.getRetryPolicy())
                                .as("metadata-only query must not use fault-tolerant execution: %s", query.getQuery())
                                .isEqualTo(RetryPolicy.NONE));
    }

    private Optional<QueryState> queryState(DistributedQueryRunner queryRunner, String queryText)
    {
        return queryRunner.getCoordinator().getQueryManager().getQueries().stream()
                .filter(query -> query.getQuery().equals(queryText))
                .collect(MoreCollectors.toOptional())
                .map(BasicQueryInfo::getState);
    }

    private void cancelQuery(DistributedQueryRunner queryRunner, String queryText)
    {
        queryRunner.getCoordinator().getQueryManager().getQueries().stream()
                .filter(query -> query.getQuery().equals(queryText))
                .forEach(query -> {
                    try {
                        queryRunner.getCoordinator().getQueryManager().cancelQuery(query.getQueryId());
                    }
                    catch (Exception e) {
                        // ignore
                    }
                });
    }
}
