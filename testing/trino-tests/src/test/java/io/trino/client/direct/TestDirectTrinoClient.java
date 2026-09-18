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
package io.trino.client.direct;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Key;
import io.trino.Session;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryManagerConfig;
import io.trino.operator.DirectExchangeClientSupplier;
import io.trino.plugin.blackhole.BlackHolePlugin;
import io.trino.plugin.exchange.filesystem.FileSystemExchangePlugin;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryFailedException;
import io.trino.testing.StandaloneQueryRunner;
import io.trino.testing.TestingDirectTrinoClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.SystemSessionProperties.DIRECT_TRINO_CLIENT_FAULT_TOLERANT_EXECUTION_ENABLED;
import static io.trino.SystemSessionProperties.RETRY_POLICY;
import static io.trino.operator.RetryPolicy.NONE;
import static io.trino.operator.RetryPolicy.QUERY;
import static io.trino.operator.RetryPolicy.TASK;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestDirectTrinoClient
{
    private static final String CLIENT_TIMEOUT = "2s";

    private StandaloneQueryRunner queryRunner;
    private StandaloneQueryRunner queryRunnerWithTaskRetry;
    private Path exchangeBaseDirectory;
    private Path taskRetryExchangeBaseDirectory;

    @BeforeAll
    public void setup()
            throws IOException
    {
        exchangeBaseDirectory = createTempDirectory("exchange_manager");
        queryRunner = new StandaloneQueryRunner(
                TEST_SESSION,
                builder -> builder.overrideProperties(ImmutableMap.of(
                        "query.client.timeout", CLIENT_TIMEOUT)));
        queryRunner.installPlugin(new BlackHolePlugin());
        queryRunner.createCatalog("blackhole", "blackhole");
        queryRunner.execute("CREATE SCHEMA blackhole.test_schema");
        queryRunner.execute("CREATE TABLE blackhole.test_schema.slow_test_table (col1 VARCHAR, col2 INTEGER)" +
                "WITH (" +
                "   split_count = 1, " +
                "   pages_per_split = 1, " +
                "   rows_per_page = 1, " +
                "   page_processing_delay = '3s'" +
                ")");
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of("tpch.splits-per-node", "1"));
        queryRunner.installPlugin(new MemoryPlugin());
        queryRunner.createCatalog("memory", "memory");
        queryRunner.installPlugin(new FileSystemExchangePlugin());
        queryRunner.loadExchangeManager("filesystem", ImmutableMap.of("exchange.base-directories", exchangeBaseDirectory.toString()));

        taskRetryExchangeBaseDirectory = createTempDirectory("exchange_manager_task_retry");
        queryRunnerWithTaskRetry = new StandaloneQueryRunner(
                TEST_SESSION,
                builder -> builder.overrideProperties(ImmutableMap.of(
                        "query.client.timeout", CLIENT_TIMEOUT,
                        "retry-policy", "TASK")));
        queryRunnerWithTaskRetry.installPlugin(new TpchPlugin());
        queryRunnerWithTaskRetry.createCatalog("tpch", "tpch", ImmutableMap.of("tpch.splits-per-node", "1"));
        queryRunnerWithTaskRetry.installPlugin(new FileSystemExchangePlugin());
        queryRunnerWithTaskRetry.loadExchangeManager("filesystem", ImmutableMap.of("exchange.base-directories", taskRetryExchangeBaseDirectory.toString()));
    }

    @AfterAll
    public void teardown()
            throws IOException
    {
        if (queryRunner != null) {
            queryRunner.close();
            queryRunner = null;
        }
        if (queryRunnerWithTaskRetry != null) {
            queryRunnerWithTaskRetry.close();
            queryRunnerWithTaskRetry = null;
        }
        if (exchangeBaseDirectory != null) {
            deleteRecursively(exchangeBaseDirectory, ALLOW_INSECURE);
            exchangeBaseDirectory = null;
        }
        if (taskRetryExchangeBaseDirectory != null) {
            deleteRecursively(taskRetryExchangeBaseDirectory, ALLOW_INSECURE);
            taskRetryExchangeBaseDirectory = null;
        }
    }

    @Test
    @Timeout(value = 20, unit = TimeUnit.SECONDS)
    public void testDirectTrinoClientLongQuery()
    {
        queryRunner.execute(TEST_SESSION, "SELECT * FROM blackhole.test_schema.slow_test_table");
    }

    @Test
    public void testBasicQuery()
    {
        MaterializedResult result = queryRunner.execute(TEST_SESSION, "SELECT 1 AS col");

        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
        assertThat(result.getColumnNames()).containsExactly("col");
    }

    @Test
    public void testEmptyResult()
    {
        MaterializedResult result = queryRunner.execute(TEST_SESSION, "SELECT * FROM (SELECT 'hello' AS col) WHERE 1 = 0");

        assertThat(result.getRowCount()).isEqualTo(0);
        assertThat(result.getColumnNames()).containsExactly("col");
    }

    @Test
    public void testDdlStatement()
    {
        Session session = Session.builder(TEST_SESSION)
                .setCatalog("memory")
                .setSchema("default")
                .build();

        String tableName = "test_table_" + randomNameSuffix();
        queryRunner.execute(session, "CREATE TABLE %s (id BIGINT)".formatted(tableName));
        assertThat(queryRunner.tableExists(session, tableName)).isTrue();

        queryRunner.execute(session, "DROP TABLE %s".formatted(tableName));
        assertThat(queryRunner.tableExists(session, tableName)).isFalse();
    }

    @Test
    public void testQueryFailure()
    {
        assertThatThrownBy(() -> queryRunner.execute(TEST_SESSION, "SELECT * FROM non_existent_table"))
                .isInstanceOf(QueryFailedException.class)
                .hasMessageContaining("non_existent_table");
    }

    @Test
    public void testUpdateStatement()
    {
        Session session = Session.builder(TEST_SESSION)
                .setCatalog("memory")
                .setSchema("default")
                .build();

        String tableName = "test_table_" + randomNameSuffix();
        queryRunner.execute(session, "CREATE TABLE %s (id BIGINT)".formatted(tableName));
        try {
            MaterializedResult result = queryRunner.execute(session, "INSERT INTO %s (id) VALUES (1), (2), (3)".formatted(tableName));
            assertThat(result.getUpdateCount()).hasValue(3L);
        }
        finally {
            queryRunner.execute(session, "DROP TABLE IF EXISTS %s".formatted(tableName));
        }
    }

    @Test
    public void testQueryWithTaskRetryPolicyInSession()
    {
        Session session = Session.builder(TEST_SESSION)
                .setSystemProperty(RETRY_POLICY, TASK.name())
                .build();

        MaterializedResult result = queryRunner.execute(session, "SELECT 1 AS col");

        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
    }

    @Test
    public void testQueryWithTaskRetryPolicyInConfig()
    {
        MaterializedResult result = queryRunnerWithTaskRetry.execute(TEST_SESSION, "SELECT 1 AS col");

        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
    }

    @Test
    public void testQueryWithQueryRetryPolicy()
    {
        Session session = Session.builder(TEST_SESSION)
                .setSystemProperty(RETRY_POLICY, QUERY.name())
                .build();

        MaterializedResult result = queryRunner.execute(session, "SELECT 1 AS col");

        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo(1);
    }

    @Test
    public void testSelectWithoutFaultTolerantExecution()
    {
        Session session = Session.builder(TEST_SESSION)
                .setSystemProperty(RETRY_POLICY, NONE.name())
                .build();

        MaterializedResult result = queryRunner.execute(session, "SELECT nationkey, name FROM tpch.tiny.nation");
        assertThat(result.getMaterializedRows()).hasSize(25);
    }

    @Test
    public void testSelectUnderTaskRetryPolicy()
    {
        // Simple scan-only query with a spooled output stage, consumed by DirectTrinoClient through the exchange manager.
        MaterializedResult result = queryRunner.execute(taskRetrySession(), "SELECT nationkey, name FROM tpch.tiny.nation");
        assertThat(result.getMaterializedRows()).hasSize(25);
    }

    @Test
    public void testAggregationUnderTaskRetryPolicy()
    {
        // Multi-stage query to make sure intermediate and output exchanges are all spooled.
        MaterializedResult result = queryRunner.execute(taskRetrySession(), "SELECT regionkey, count(*) FROM tpch.tiny.nation GROUP BY regionkey");
        assertThat(result.getMaterializedRows()).hasSize(5);
    }

    @Test
    public void testLargeResultUnderTaskRetryPolicy()
    {
        // A result set large enough to be spooled across many pages/chunks through the exchange, so the
        // consume loop in DirectTrinoClient must repeatedly poll and deserialize from external storage.
        // lineitem@tiny has 60175 rows and ~16 columns, i.e. several MB of spooled output.
        MaterializedResult result = queryRunner.execute(taskRetrySession(), "SELECT * FROM tpch.tiny.lineitem");
        assertThat(result.getMaterializedRows()).hasSize(60175);
    }

    @Test
    public void testFaultTolerantExecutionOptOutForcesRetryPolicyNone()
    {
        // Opting out restores the legacy behavior: a TASK-retry request is downgraded to NONE so DTC consumes
        // the streaming exchange. Build the client directly to inspect the query's resolved retry policy.
        Session session = Session.builder(TEST_SESSION)
                .setSystemProperty(RETRY_POLICY, TASK.name())
                .setSystemProperty(DIRECT_TRINO_CLIENT_FAULT_TOLERANT_EXECUTION_ENABLED, "false")
                .build();

        TestingTrinoServer coordinator = queryRunner.getCoordinator();
        TestingDirectTrinoClient directClient = new TestingDirectTrinoClient(
                coordinator.getDispatchManager(),
                coordinator.getQueryManager(),
                coordinator.getInstance(Key.get(QueryManagerConfig.class)),
                coordinator.getInstance(Key.get(DirectExchangeClientSupplier.class)),
                coordinator.getInstance(Key.get(ExchangeManagerRegistry.class)),
                coordinator.getInstance(Key.get(BlockEncodingSerde.class)));

        TestingDirectTrinoClient.Result result = directClient.execute(session, "SELECT nationkey, name FROM tpch.tiny.nation");
        assertThat(result.result().get().getMaterializedRows()).hasSize(25);

        QueryInfo queryInfo = coordinator.getQueryManager().getFullQueryInfo(result.queryId());
        assertThat(queryInfo.getSession().getSystemProperties())
                .containsEntry(RETRY_POLICY, NONE.name());
    }

    private Session taskRetrySession()
    {
        return Session.builder(TEST_SESSION)
                .setSystemProperty(RETRY_POLICY, TASK.name())
                .build();
    }
}
