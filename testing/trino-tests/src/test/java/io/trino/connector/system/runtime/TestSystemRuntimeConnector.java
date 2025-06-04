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
package io.trino.connector.system.runtime;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static com.google.common.collect.MoreCollectors.toOptional;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public class TestSystemRuntimeConnector
        extends AbstractTestQueryFramework
{
    private static final Function<SchemaTableName, List<ColumnMetadata>> DEFAULT_GET_COLUMNS = table -> ImmutableList.of(new ColumnMetadata("c", VARCHAR));
    private static final AtomicLong counter = new AtomicLong();

    private static Function<SchemaTableName, List<ColumnMetadata>> getColumns;

    private final ExecutorService executor = Executors.newSingleThreadScheduledExecutor(threadsNamed(TestSystemRuntimeConnector.class.getSimpleName()));

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("mock")
                .setSchema("default")
                .build();

        QueryRunner queryRunner = DistributedQueryRunner
                .builder(defaultSession)
                .enableBackupCoordinator()
                .setWorkerCount(1)
                .build();
        queryRunner.installPlugin(new Plugin()
        {
            @Override
            public Iterable<ConnectorFactory> getConnectorFactories()
            {
                MockConnectorFactory connectorFactory = MockConnectorFactory.builder()
                        .withGetViews((session, schemaTablePrefix) -> ImmutableMap.of())
                        .withListTables((session, s) -> ImmutableList.of("test_table"))
                        .withGetColumns(tableName -> getColumns.apply(tableName))
                        .build();
                return ImmutableList.of(connectorFactory);
            }
        });
        queryRunner.createCatalog("mock", "mock", ImmutableMap.of());
        return queryRunner;
    }

    @AfterAll
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testRuntimeNodes()
    {
        assertQuery(
                "SELECT node_version, coordinator, state FROM system.runtime.nodes",
                "VALUES " +
                        "('testversion', true, 'active')," +
                        "('testversion', true, 'active')," + // backup coordinator
                        "('testversion', false, 'active')");
    }

    @Test
    void testOptimizerRuleStats()
    {
        assertThat(query("SELECT rule_name, invocations, matches, failures FROM system.runtime.optimizer_rule_stats"))
                .result().hasTypes(ImmutableList.of(VARCHAR, BIGINT, BIGINT, BIGINT));
    }

    @Test
    public void testRuntimeQueriesTimestamps()
    {
        // Test is run multiple times because it is vulnerable to OS clock adjustment. See https://github.com/trinodb/trino/issues/5608
        run(10, 0.8, () -> {
            ZonedDateTime timeBefore = ZonedDateTime.now();
            computeActual("SELECT 1");
            MaterializedResult result = computeActual("" +
                    "SELECT max(created), max(started), max(last_heartbeat), max(\"end\") " +
                    "FROM system.runtime.queries");
            ZonedDateTime timeAfter = ZonedDateTime.now();

            MaterializedRow row = Iterables.getOnlyElement(result.toTestTypes().getMaterializedRows());
            List<Object> fields = row.getFields();
            assertThat(fields).hasSize(4);
            for (int i = 0; i < fields.size(); i++) {
                Object value = fields.get(i);
                assertThat((ZonedDateTime) value)
                        .as("value for field " + i)
                        .isNotNull()
                        .isAfterOrEqualTo(timeBefore)
                        .isBeforeOrEqualTo(timeAfter);
            }
        });
    }

    @Test
    public void testRuntimeTasksTimestamps()
    {
        // Test is run multiple times because it is vulnerable to OS clock adjustment. See https://github.com/trinodb/trino/issues/5608
        run(10, 0.8, () -> {
            ZonedDateTime timeBefore = ZonedDateTime.now();
            computeActual("SELECT 1");
            MaterializedResult result = computeActual("" +
                    "SELECT max(created), max(start), max(last_heartbeat), max(\"end\") " +
                    "FROM system.runtime.tasks");
            ZonedDateTime timeAfter = ZonedDateTime.now();

            MaterializedRow row = Iterables.getOnlyElement(result.toTestTypes().getMaterializedRows());
            List<Object> fields = row.getFields();
            assertThat(fields).hasSize(4);
            for (int i = 0; i < fields.size(); i++) {
                Object value = fields.get(i);
                assertThat((ZonedDateTime) value)
                        .as("value for field " + i)
                        .isNotNull()
                        .isAfterOrEqualTo(timeBefore)
                        .isBeforeOrEqualTo(timeAfter);
            }
        });
    }

    // Test is run multiple times because it is vulnerable to OS clock adjustment. See https://github.com/trinodb/trino/issues/5608
    @Test
    public void testRuntimeTransactionsTimestamps()
    {
        run(10, 0.8, () -> {
            ZonedDateTime timeBefore = ZonedDateTime.now();
            computeActual("START TRANSACTION");
            MaterializedResult result = computeActual("" +
                    "SELECT max(create_time) " +
                    "FROM system.runtime.transactions");
            ZonedDateTime timeAfter = ZonedDateTime.now();

            MaterializedRow row = Iterables.getOnlyElement(result.toTestTypes().getMaterializedRows());
            List<Object> fields = row.getFields();
            assertThat(fields).hasSize(1);
            for (int i = 0; i < fields.size(); i++) {
                Object value = fields.get(i);
                assertThat((ZonedDateTime) value)
                        .as("value for field " + i)
                        .isNotNull()
                        .isAfterOrEqualTo(timeBefore)
                        .isBeforeOrEqualTo(timeAfter);
            }
        });
    }

    @Test
    public void testFinishedQueryIsCaptured()
    {
        getColumns = DEFAULT_GET_COLUMNS;
        String testQueryId = "test_query_id_" + counter.incrementAndGet();
        getQueryRunner().execute(format("EXPLAIN SELECT 1 AS %s FROM test_table", testQueryId));

        assertQuery(
                format("SELECT state FROM system.runtime.queries WHERE query LIKE '%%%s%%' AND query NOT LIKE '%%system.runtime.queries%%'", testQueryId),
                "VALUES 'FINISHED'");
    }

    @Test
    @Timeout(60)
    public void testQueryDuringAnalysisIsCaptured()
    {
        SettableFuture<List<ColumnMetadata>> metadataFuture = SettableFuture.create();
        getColumns = schemaTableName -> {
            try {
                return metadataFuture.get();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        String testQueryId = "test_query_id_" + counter.incrementAndGet();
        Future<?> queryFuture = executor.submit(() -> {
            getQueryRunner().execute(format("EXPLAIN SELECT 1 AS %s FROM test_table", testQueryId));
        });

        assertQueryEventually(
                getSession(),
                format("SELECT state FROM system.runtime.queries WHERE query LIKE '%%%s%%' AND query NOT LIKE '%%system.runtime.queries%%'", testQueryId),
                "VALUES 'WAITING_FOR_RESOURCES'",
                new Duration(10, SECONDS));
        assertThat(metadataFuture.isDone()).isFalse();
        assertThat(queryFuture.isDone()).isFalse();

        metadataFuture.set(ImmutableList.of(new ColumnMetadata("a", BIGINT)));

        assertQueryEventually(
                getSession(),
                format("SELECT state FROM system.runtime.queries WHERE query LIKE '%%%s%%' AND query NOT LIKE '%%system.runtime.queries%%'", testQueryId),
                "VALUES 'FINISHED'",
                new Duration(10, SECONDS));
        // Client should receive query result immediately afterwards
        assertEventually(new Duration(5, SECONDS), () -> assertThat(queryFuture.isDone()).isTrue());
    }

    @Test
    @Timeout(60)
    public void testQueryKillingDuringAnalysis()
    {
        SettableFuture<List<ColumnMetadata>> metadataFuture = SettableFuture.create();
        getColumns = schemaTableName -> {
            try {
                return metadataFuture.get();
            }
            catch (InterruptedException e) {
                metadataFuture.cancel(true);
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        };
        String testQueryId = "test_query_id_" + counter.incrementAndGet();
        Future<?> queryFuture = executor.submit(() -> {
            getQueryRunner().execute(format("EXPLAIN SELECT 1 AS %s FROM test_table", testQueryId));
        });

        // Wait for query to start
        assertQueryEventually(
                getSession(),
                format("SELECT count(*) FROM system.runtime.queries WHERE query LIKE '%%%s%%' AND query NOT LIKE '%%system.runtime.queries%%'", testQueryId),
                "VALUES 1",
                new Duration(5, SECONDS));

        Optional<Object> queryId = computeActual(format("SELECT query_id FROM system.runtime.queries WHERE query LIKE '%%%s%%' AND query NOT LIKE '%%system.runtime.queries%%'", testQueryId))
                .getOnlyColumn()
                .collect(toOptional());
        assertThat(metadataFuture.isDone()).isFalse();
        assertThat(queryFuture.isDone()).isFalse();
        assertThat(queryId).isPresent();

        getQueryRunner().execute(format("CALL system.runtime.kill_query('%s', 'because')", queryId.get()));
        // Cancellation should happen within kill_query, but it still needs to be propagated to the thread performing analysis.
        assertEventually(new Duration(5, SECONDS), () -> assertThat(metadataFuture.isCancelled()).isTrue());
        // Client should receive query result (failure) immediately afterwards
        assertEventually(new Duration(5, SECONDS), () -> assertThat(queryFuture.isDone()).isTrue());
    }

    @Test
    public void testTasksTable()
    {
        getQueryRunner().execute("SELECT 1");
        getQueryRunner().execute("SELECT * FROM system.runtime.tasks");
    }

    private static void run(int repetitions, double successRate, Runnable test)
    {
        AssertionError lastError = null;
        int failures = 0;
        for (int iteration = 0; iteration < repetitions; iteration++) {
            try {
                test.run();
            }
            catch (AssertionError e) {
                failures++;
                lastError = e;
            }
        }

        if (lastError != null && 1 - (failures * 1.0) / repetitions < successRate) {
            throw lastError;
        }
    }
}
