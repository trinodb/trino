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
package io.trino.testing;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.graph.Traverser;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.client.StageStats;
import io.trino.client.StatementStats;
import io.trino.execution.FailureInjector.InjectedFailureType;
import io.trino.operator.RetryPolicy;
import io.trino.spi.ErrorType;
import io.trino.spi.QueryId;
import io.trino.tpch.TpchTable;
import org.assertj.core.api.AbstractThrowableAssert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.google.common.base.Functions.identity;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;
import static io.trino.execution.FailureInjector.FAILURE_INJECTION_MESSAGE;
import static io.trino.execution.FailureInjector.InjectedFailureType.TASK_FAILURE;
import static io.trino.execution.FailureInjector.InjectedFailureType.TASK_GET_RESULTS_REQUEST_FAILURE;
import static io.trino.execution.FailureInjector.InjectedFailureType.TASK_GET_RESULTS_REQUEST_TIMEOUT;
import static io.trino.execution.FailureInjector.InjectedFailureType.TASK_MANAGEMENT_REQUEST_FAILURE;
import static io.trino.execution.FailureInjector.InjectedFailureType.TASK_MANAGEMENT_REQUEST_TIMEOUT;
import static io.trino.plugin.base.TemporaryTables.temporaryTableNamePrefix;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.SUPPLIER;
import static java.lang.Integer.parseInt;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.data.Percentage.withPercentage;
import static org.testng.Assert.assertEquals;

public abstract class BaseFailureRecoveryTest
        extends AbstractTestQueryFramework
{
    protected static final int INVOCATION_COUNT = 1;
    private static final Duration MAX_ERROR_DURATION = new Duration(5, SECONDS);
    private static final Duration REQUEST_TIMEOUT = new Duration(5, SECONDS);
    private static final int DEFAULT_MAX_PARALLEL_TEST_CONCURRENCY = 4;

    private final RetryPolicy retryPolicy;
    private final Semaphore parallelTestsSemaphore;

    protected BaseFailureRecoveryTest(RetryPolicy retryPolicy)
    {
        this(retryPolicy, DEFAULT_MAX_PARALLEL_TEST_CONCURRENCY);
    }

    protected BaseFailureRecoveryTest(RetryPolicy retryPolicy, int maxParallelTestConcurrency)
    {
        this.retryPolicy = requireNonNull(retryPolicy, "retryPolicy is null");
        this.parallelTestsSemaphore = new Semaphore(maxParallelTestConcurrency);
    }

    protected RetryPolicy getRetryPolicy()
    {
        return retryPolicy;
    }

    @Override
    protected final QueryRunner createQueryRunner()
            throws Exception
    {
        return createQueryRunner(
                ImmutableList.of(NATION, ORDERS, CUSTOMER, SUPPLIER),
                ImmutableMap.<String, String>builder()
                        .put("query.remote-task.max-error-duration", MAX_ERROR_DURATION.toString())
                        .put("exchange.max-error-duration", MAX_ERROR_DURATION.toString())
                        .put("retry-policy", retryPolicy.toString())
                        .put("retry-initial-delay", "0s")
                        .put("query-retry-attempts", "1")
                        .put("failure-injection.request-timeout", new Duration(REQUEST_TIMEOUT.toMillis() * 2, MILLISECONDS).toString())
                        // making http timeouts shorter so tests which simulate communication timeouts finish in reasonable amount of time
                        .put("exchange.http-client.idle-timeout", REQUEST_TIMEOUT.toString())
                        .put("fault-tolerant-execution-partition-count", "5")
                        // to trigger spilling
                        .put("exchange.deduplication-buffer-size", "1kB")
                        .put("fault-tolerant-execution-task-memory", "1GB")
                        .buildOrThrow(),
                ImmutableMap.of(
                        // making http timeouts shorter so tests which simulate communication timeouts finish in reasonable amount of time
                        "scheduler.http-client.idle-timeout", REQUEST_TIMEOUT.toString()));
    }

    protected abstract QueryRunner createQueryRunner(
            List<TpchTable<?>> requiredTpchTables,
            Map<String, String> configProperties,
            Map<String, String> coordinatorProperties)
            throws Exception;

    protected abstract boolean areWriteRetriesSupported();

    protected void testSelect(String query)
    {
        testSelect(query, Optional.empty());
    }

    protected void testSelect(String query, Optional<Session> session)
    {
        testSelect(query, session, queryId -> {});
    }

    protected void testSelect(String query, Optional<Session> session, Consumer<QueryId> queryAssertion)
    {
        assertThatQuery(query)
                .withSession(session)
                .experiencing(TASK_MANAGEMENT_REQUEST_FAILURE)
                .at(leafStage())
                .failsWithoutRetries(failure -> failure.hasMessageFindingMatch("Error 500 Internal Server Error|Error closing remote buffer, expected 204 got 500"))
                .finishesSuccessfully(queryAssertion);

        assertThatQuery(query)
                .experiencing(TASK_MANAGEMENT_REQUEST_TIMEOUT)
                .at(distributedStage())
                .failsWithoutRetries(failure -> failure.hasMessageContaining("Encountered too many errors talking to a worker node"))
                .finishesSuccessfully();

        assertThatQuery(query)
                .withSession(session)
                .experiencing(TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR))
                .at(leafStage())
                .failsWithoutRetries(failure -> failure.hasMessageContaining(FAILURE_INJECTION_MESSAGE))
                .finishesSuccessfully(queryAssertion);

        assertThatQuery(query)
                .withSession(session)
                .experiencing(TASK_FAILURE, Optional.of(ErrorType.EXTERNAL))
                .at(distributedStage())
                .failsWithoutRetries(failure -> failure.hasMessageContaining(FAILURE_INJECTION_MESSAGE))
                .finishesSuccessfully(queryAssertion);

        if (getRetryPolicy() == RetryPolicy.QUERY) {
            assertThatQuery(query)
                    .withSession(session)
                    .experiencing(TASK_GET_RESULTS_REQUEST_FAILURE)
                    .at(boundaryDistributedStage())
                    .failsWithoutRetries(failure -> failure.hasMessageFindingMatch("Error 500 Internal Server Error|Error closing remote buffer, expected 204 got 500"))
                    .finishesSuccessfully(queryAssertion);

            assertThatQuery(query)
                    .experiencing(TASK_GET_RESULTS_REQUEST_TIMEOUT)
                    // using boundary stage so we observe task failures
                    .at(boundaryDistributedStage())
                    .failsWithoutRetries(failure -> failure.hasMessageFindingMatch("Encountered too many errors talking to a worker node|Error closing remote buffer"))
                    .finishesSuccessfully();
        }
    }

    @DataProvider(name = "parallelTests", parallel = true)
    public Object[][] parallelTests()
    {
        return new Object[][] {
                parallelTest("testCreateTable", this::testCreateTable),
                parallelTest("testInsert", this::testInsert),
                parallelTest("testDelete", this::testDelete),
                parallelTest("testDeleteWithSubquery", this::testDeleteWithSubquery),
                parallelTest("testUpdate", this::testUpdate),
                parallelTest("testUpdateWithSubquery", this::testUpdateWithSubquery),
                parallelTest("testMerge", this::testMerge),
                parallelTest("testRefreshMaterializedView", this::testRefreshMaterializedView),
                parallelTest("testAnalyzeTable", this::testAnalyzeTable),
                parallelTest("testExplainAnalyze", this::testExplainAnalyze),
                parallelTest("testRequestTimeouts", this::testRequestTimeouts)
        };
    }

    @Test(invocationCount = INVOCATION_COUNT, dataProvider = "parallelTests")
    public final void testParallel(Runnable runnable)
    {
        try {
            // By default, a test method using a @DataProvider with parallel attribute is run in 10 threads (org.testng.xml.XmlSuite#DEFAULT_DATA_PROVIDER_THREAD_COUNT).
            // We limit number of concurrent test executions to prevent excessive resource usage.
            //
            // Note: the downside of this approach is that individual test runtimes will not be representative anymore
            //       as those will include time spent waiting for semaphore.
            parallelTestsSemaphore.acquire();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        try {
            runnable.run();
        }
        finally {
            parallelTestsSemaphore.release();
        }
    }

    protected void testCreateTable()
    {
        testTableModification(
                Optional.empty(),
                "CREATE TABLE <table> AS SELECT * FROM orders",
                Optional.of("DROP TABLE <table>"));
    }

    protected void testInsert()
    {
        testTableModification(
                Optional.of("CREATE TABLE <table> AS SELECT * FROM orders WITH NO DATA"),
                "INSERT INTO <table> SELECT * FROM orders",
                Optional.of("DROP TABLE <table>"));
    }

    protected void testDelete()
    {
        testTableModification(
                Optional.of("CREATE TABLE <table> AS SELECT * FROM orders"),
                "DELETE FROM <table> WHERE orderkey = 1",
                Optional.of("DROP TABLE <table>"));
    }

    protected void testDeleteWithSubquery()
    {
        testTableModification(
                Optional.of("CREATE TABLE <table> AS SELECT * FROM orders"),
                "DELETE FROM <table> WHERE custkey IN (SELECT custkey FROM customer WHERE nationkey = 1)",
                Optional.of("DROP TABLE <table>"));
    }

    protected void testUpdate()
    {
        testTableModification(
                Optional.of("CREATE TABLE <table> AS SELECT * FROM orders"),
                "UPDATE <table> SET shippriority = 101 WHERE custkey = 1",
                Optional.of("DROP TABLE <table>"));
    }

    protected void testUpdateWithSubquery()
    {
        testTableModification(
                Optional.of("CREATE TABLE <table> AS SELECT * FROM orders"),
                "UPDATE <table> SET shippriority = 101 WHERE custkey = (SELECT min(custkey) FROM customer)",
                Optional.of("DROP TABLE <table>"));
    }

    protected void testAnalyzeTable()
    {
        testNonSelect(
                Optional.empty(),
                Optional.of("CREATE TABLE <table> AS SELECT * FROM orders"),
                "ANALYZE <table>",
                Optional.of("DROP TABLE <table>"),
                false);
    }

    protected void testMerge()
    {
        testTableModification(
                Optional.of("CREATE TABLE <table> AS SELECT * FROM orders"),
                """
                        MERGE INTO <table> t
                        USING (SELECT orderkey, 'X' clerk FROM <table>) s
                        ON t.orderkey = s.orderkey
                        WHEN MATCHED AND s.orderkey > 1000
                            THEN UPDATE SET clerk = t.clerk || s.clerk
                        WHEN MATCHED AND s.orderkey <= 1000
                            THEN DELETE
                        """,
                Optional.of("DROP TABLE <table>"));
    }

    protected void testRefreshMaterializedView()
    {
        testTableModification(
                Optional.of("CREATE MATERIALIZED VIEW <table> AS SELECT * FROM orders"),
                "REFRESH MATERIALIZED VIEW <table>",
                Optional.of("DROP MATERIALIZED VIEW <table>"));
    }

    protected void testExplainAnalyze()
    {
        testSelect("EXPLAIN ANALYZE SELECT orderStatus, count(*) FROM orders GROUP BY orderStatus");

        testTableModification(
                Optional.of("CREATE TABLE <table> AS SELECT * FROM orders WITH NO DATA"),
                "EXPLAIN ANALYZE INSERT INTO <table> SELECT * FROM orders",
                Optional.of("DROP TABLE <table>"));
    }

    protected void testRequestTimeouts()
    {
        if (areWriteRetriesSupported()) {
            assertThatQuery("INSERT INTO <table> SELECT * FROM orders")
                    .withSetupQuery(Optional.of("CREATE TABLE <table> AS SELECT * FROM orders WITH NO DATA"))
                    .withCleanupQuery(Optional.of("DROP TABLE <table>"))
                    .experiencing(TASK_GET_RESULTS_REQUEST_TIMEOUT)
                    .at(leafStage())
                    .failsWithoutRetries(failure -> failure.hasMessageFindingMatch("Encountered too many errors talking to a worker node|Error closing remote buffer"))
                    // get results timeout for leaf stage will not result in accounted task failure if failure recovery is enabled
                    .finishesSuccessfullyWithoutTaskFailures();
        }
    }

    protected void testTableModification(Optional<String> setupQuery, String query, Optional<String> cleanupQuery)
    {
        testTableModification(Optional.empty(), setupQuery, query, cleanupQuery);
    }

    protected void testTableModification(Optional<Session> session, Optional<String> setupQuery, String query, Optional<String> cleanupQuery)
    {
        testNonSelect(session, setupQuery, query, cleanupQuery, true);
    }

    protected void testNonSelect(Optional<Session> session, Optional<String> setupQuery, String query, Optional<String> cleanupQuery, boolean writesData)
    {
        if (writesData && !areWriteRetriesSupported()) {
            // if retries are not supported assert on that and skip actual failures simulation
            assertThatQuery(query)
                    .withSession(session)
                    .withSetupQuery(setupQuery)
                    .withCleanupQuery(cleanupQuery)
                    .failsDespiteRetries(failure -> failure.hasMessageMatching("This connector does not support query retries"))
                    .cleansUpTemporaryTables();
            return;
        }

        assertThatQuery(query)
                .withSession(session)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR))
                .at(boundaryCoordinatorStage())
                .failsAlways(failure -> failure.hasMessageContaining(FAILURE_INJECTION_MESSAGE))
                .cleansUpTemporaryTables();

        assertThatQuery(query)
                .withSession(session)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR))
                .at(rootStage())
                .failsAlways(failure -> failure.hasMessageContaining(FAILURE_INJECTION_MESSAGE))
                .cleansUpTemporaryTables();

        assertThatQuery(query)
                .withSession(session)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR))
                .at(boundaryDistributedStage())
                .failsWithoutRetries(failure -> failure.hasMessageContaining(FAILURE_INJECTION_MESSAGE))
                .finishesSuccessfully()
                .cleansUpTemporaryTables();

        assertThatQuery(query)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_MANAGEMENT_REQUEST_TIMEOUT)
                .at(boundaryDistributedStage())
                .failsWithoutRetries(failure -> failure.hasMessageContaining("Encountered too many errors talking to a worker node"))
                .finishesSuccessfully()
                .cleansUpTemporaryTables();

        if (getRetryPolicy() == RetryPolicy.QUERY) {
            assertThatQuery(query)
                    .withSession(session)
                    .withSetupQuery(setupQuery)
                    .withCleanupQuery(cleanupQuery)
                    .experiencing(TASK_GET_RESULTS_REQUEST_FAILURE)
                    .at(boundaryDistributedStage())
                    .failsWithoutRetries(failure -> failure.hasMessageFindingMatch("Error 500 Internal Server Error|Error closing remote buffer, expected 204 got 500"))
                    .finishesSuccessfully()
                    .cleansUpTemporaryTables();

            assertThatQuery(query)
                    .withSetupQuery(setupQuery)
                    .withCleanupQuery(cleanupQuery)
                    .experiencing(TASK_GET_RESULTS_REQUEST_TIMEOUT)
                    .at(boundaryDistributedStage())
                    .failsWithoutRetries(failure -> failure.hasMessageFindingMatch("Encountered too many errors talking to a worker node|Error closing remote buffer"))
                    .finishesSuccessfully()
                    .cleansUpTemporaryTables();
        }
    }

    protected FailureRecoveryAssert assertThatQuery(String query)
    {
        return new FailureRecoveryAssert(query);
    }

    // Provided as a protected method here in case this is not a one-sized-fits-all solution
    protected void checkTemporaryTables(Set<String> queryIds)
    {
        // queryId -> temporary table names
        Map<String, Set<String>> remainingTemporaryTables = new HashMap<>();
        // queryId -> assertion messages
        Map<String, Set<String>> assertionErrorMessages = new HashMap<>();
        for (String queryId : queryIds) {
            String temporaryTablePrefix = temporaryTableNamePrefix(queryId);
            MaterializedResult temporaryTablesResult = getQueryRunner()
                    .execute("SHOW TABLES LIKE '%s%%' ESCAPE '\\'".formatted(temporaryTablePrefix.replace("_", "\\_")));
            // Unfortunately, information_schema is not strictly consistent with recently dropped tables,
            // and for some connectors, it can return tables that have been recently dropped. Therefore,
            // we can't rely simply on SHOW TABLES LIKE returning no results - we have to try to query the table
            for (MaterializedRow temporaryTableRow : temporaryTablesResult.getMaterializedRows()) {
                String temporaryTableName = (String) temporaryTableRow.getField(0);
                try {
                    assertThatThrownBy(() -> getQueryRunner().execute("SELECT 1 FROM %s WHERE 1 = 0".formatted(temporaryTableName)))
                            .hasMessageContaining("Table '%s' does not exist", temporaryTableName);
                }
                catch (AssertionError e) {
                    remainingTemporaryTables.computeIfAbsent(queryId, ignored -> new HashSet<>()).add(temporaryTableName);
                    assertionErrorMessages.computeIfAbsent(queryId, ignored -> new HashSet<>()).add(e.getMessage());
                }
            }
        }

        assertThat(remainingTemporaryTables.isEmpty())
                .as("There should be no remaining tmp_trino tables that are queryable. They are:\n%s",
                        remainingTemporaryTables.entrySet().stream()
                                .map(entry -> "\tFor queryId [%s] (prefix [%s]) remaining tables: [%s]\n\t\tWith errors: [%s]".formatted(
                                        entry.getKey(),
                                        temporaryTableNamePrefix(entry.getKey()),
                                        Joiner.on(",").join(entry.getValue()),
                                        Joiner.on("],\n[").join(assertionErrorMessages.get(entry.getKey())).replace("\n", "\n\t\t\t")))
                                .collect(joining("\n")))
                .isTrue();
    }

    protected class FailureRecoveryAssert
    {
        private final String query;
        private Session session = getQueryRunner().getDefaultSession();
        private Optional<Function<MaterializedResult, Integer>> stageSelector = Optional.empty();
        private Optional<InjectedFailureType> failureType = Optional.empty();
        private Optional<ErrorType> errorType = Optional.empty();
        private Optional<String> setup = Optional.empty();
        private Optional<String> cleanup = Optional.empty();
        private Set<String> queryIds = new HashSet<>();

        public FailureRecoveryAssert(String query)
        {
            this.query = requireNonNull(query, "query is null");
        }

        public FailureRecoveryAssert withSession(Optional<Session> session)
        {
            requireNonNull(session, "session is null");
            session.ifPresent(value -> this.session = value);
            return this;
        }

        public FailureRecoveryAssert withSetupQuery(Optional<String> query)
        {
            setup = requireNonNull(query, "query is null");
            return this;
        }

        public FailureRecoveryAssert withCleanupQuery(Optional<String> query)
        {
            cleanup = requireNonNull(query, "query is null");
            return this;
        }

        public FailureRecoveryAssert experiencing(InjectedFailureType failureType)
        {
            return experiencing(failureType, Optional.empty());
        }

        public FailureRecoveryAssert experiencing(InjectedFailureType failureType, Optional<ErrorType> errorType)
        {
            this.failureType = Optional.of(requireNonNull(failureType, "failureType is null"));
            this.errorType = requireNonNull(errorType, "errorType is null");
            if (failureType == TASK_FAILURE) {
                checkArgument(errorType.isPresent(), "error type must be present when injection type is task failure");
            }
            else {
                checkArgument(errorType.isEmpty(), "error type must not be present when injection type is not task failure");
            }
            return this;
        }

        public FailureRecoveryAssert at(Function<MaterializedResult, Integer> stageSelector)
        {
            this.stageSelector = Optional.of(requireNonNull(stageSelector, "stageSelector is null"));
            return this;
        }

        private ExecutionResult executeExpected()
        {
            return execute(noRetries(session), query, Optional.empty());
        }

        private ExecutionResult executeActual(OptionalInt failureStageId)
        {
            return executeActual(session, failureStageId);
        }

        private ExecutionResult executeActualNoRetries(OptionalInt failureStageId)
        {
            return executeActual(noRetries(session), failureStageId);
        }

        private ExecutionResult executeActual(Session session, OptionalInt failureStageId)
        {
            String token = UUID.randomUUID().toString();
            if (failureType.isPresent()) {
                getQueryRunner().injectTaskFailure(
                        token,
                        failureStageId.orElseThrow(() -> new IllegalArgumentException("failure stageId not provided")),
                        0,
                        0,
                        failureType.get(),
                        errorType);

                return execute(session, query, Optional.of(token));
            }
            // no failure injected
            return execute(session, query, Optional.of(token));
        }

        private ExecutionResult execute(Session session, String query, Optional<String> traceToken)
        {
            String tableName = "table_" + randomNameSuffix();
            setup.ifPresent(sql -> getQueryRunner().execute(noRetries(session), resolveTableName(sql, tableName)));

            MaterializedResultWithQueryId resultWithQueryId = null;
            RuntimeException failure = null;
            String queryId = null;
            try {
                resultWithQueryId = getDistributedQueryRunner().executeWithQueryId(withTraceToken(session, traceToken), resolveTableName(query, tableName));
                queryId = resultWithQueryId.getQueryId().getId();
            }
            catch (RuntimeException e) {
                failure = e;
                if (e instanceof QueryFailedException) {
                    queryId = ((QueryFailedException) e).getQueryId().getId();
                }
            }

            if (queryId != null) {
                queryIds.add(queryId);
            }

            MaterializedResult result = resultWithQueryId == null ? null : resultWithQueryId.getResult();
            Optional<MaterializedResult> updatedTableContent = Optional.empty();
            if (result != null && result.getUpdateCount().isPresent()) {
                updatedTableContent = Optional.of(getQueryRunner().execute(noRetries(session), "SELECT * FROM " + tableName));
            }

            Optional<MaterializedResult> updatedTableStatistics = Optional.empty();
            if (result != null && result.getUpdateType().isPresent() && result.getUpdateType().get().equals("ANALYZE")) {
                updatedTableStatistics = Optional.of(getQueryRunner().execute(noRetries(session), "SHOW STATS FOR " + tableName));
            }

            try {
                cleanup.ifPresent(sql -> getQueryRunner().execute(noRetries(session), resolveTableName(sql, tableName)));
            }
            catch (RuntimeException e) {
                if (failure == null) {
                    failure = e;
                }
                else if (failure != e) {
                    failure.addSuppressed(e);
                }
            }

            if (failure != null) {
                throw failure;
            }

            return new ExecutionResult(resultWithQueryId, updatedTableContent, updatedTableStatistics);
        }

        public void isCoordinatorOnly()
        {
            verifyFailureTypeAndStageSelector();
            ExecutionResult result = executeExpected();
            StageStats rootStage = result.getQueryResult().getStatementStats().get().getRootStage();
            List<StageStats> subStages = rootStage.getSubStages();

            assertThat(rootStage.isCoordinatorOnly()).isTrue();
            assertThat(subStages).isEmpty();
        }

        public FailureRecoveryAssert cleansUpTemporaryTables()
        {
            checkTemporaryTables(queryIds);
            return this;
        }

        public FailureRecoveryAssert finishesSuccessfully()
        {
            return finishesSuccessfully(queryId -> {});
        }

        public FailureRecoveryAssert finishesSuccessfullyWithoutTaskFailures()
        {
            return finishesSuccessfully(queryId -> {}, false);
        }

        private FailureRecoveryAssert finishesSuccessfully(Consumer<QueryId> queryAssertion)
        {
            return finishesSuccessfully(queryAssertion, true);
        }

        public FailureRecoveryAssert finishesSuccessfully(Consumer<QueryId> queryAssertion, boolean expectTaskFailures)
        {
            verifyFailureTypeAndStageSelector();
            ExecutionResult expected = executeExpected();
            MaterializedResult expectedQueryResult = expected.getQueryResult();
            OptionalInt failureStageId = getFailureStageId(() -> expectedQueryResult);
            ExecutionResult actual = executeActual(failureStageId);
            assertEquals(getStageStats(actual.getQueryResult(), failureStageId.getAsInt()).getFailedTasks(), expectTaskFailures ? 1 : 0);
            MaterializedResult actualQueryResult = actual.getQueryResult();

            boolean isAnalyze = expectedQueryResult.getUpdateType().isPresent() && expectedQueryResult.getUpdateType().get().equals("ANALYZE");
            boolean isUpdate = expectedQueryResult.getUpdateCount().isPresent();
            boolean isExplain = query.trim().toUpperCase(ENGLISH).startsWith("EXPLAIN");
            if (isAnalyze) {
                assertEquals(actualQueryResult.getUpdateCount(), expectedQueryResult.getUpdateCount());
                assertThat(expected.getUpdatedTableStatistics()).isPresent();
                assertThat(actual.getUpdatedTableStatistics()).isPresent();

                MaterializedResult expectedUpdatedTableStatisticsResult = expected.getUpdatedTableStatistics().get();
                MaterializedResult actualUpdatedTableStatisticsResult = actual.getUpdatedTableStatistics().get();
                assertEquals(actualUpdatedTableStatisticsResult.getTypes(), expectedUpdatedTableStatisticsResult.getTypes(), "Column types");
                assertEquals(actualUpdatedTableStatisticsResult.getColumnNames(), expectedUpdatedTableStatisticsResult.getColumnNames(), "Column names");
                Map<String, MaterializedRow> expectedUpdatedTableStatistics = expectedUpdatedTableStatisticsResult.getMaterializedRows().stream()
                        .collect(toMap(row -> (String) row.getField(0), identity()));
                Map<String, MaterializedRow> actualUpdatedTableStatistics = actualUpdatedTableStatisticsResult.getMaterializedRows().stream()
                        .collect(toMap(row -> (String) row.getField(0), identity()));
                assertEquals(actualUpdatedTableStatistics.keySet(), expectedUpdatedTableStatistics.keySet(), "Table columns");
                expectedUpdatedTableStatistics.forEach((key, expectedRow) -> {
                    MaterializedRow actualRow = actualUpdatedTableStatistics.get(key);
                    assertEquals(actualRow.getFieldCount(), expectedRow.getFieldCount(), "Unexpected layout of stats");
                    for (int statsColumnIndex = 0; statsColumnIndex < expectedRow.getFieldCount(); statsColumnIndex++) {
                        String statsColumnName = actualUpdatedTableStatisticsResult.getColumnNames().get(statsColumnIndex);
                        String testedFieldDescription = "Field %d '%s' in %s".formatted(statsColumnIndex, statsColumnName, actualRow);
                        Object expectedValue = expectedRow.getField(statsColumnIndex);
                        Object actualValue = actualRow.getField(statsColumnIndex);
                        if (expectedValue == null) {
                            assertThat(actualValue).as(testedFieldDescription)
                                    .isNull();
                        }
                        else {
                            switch (statsColumnName) {
                                case "data_size", "distinct_values_count" -> {
                                    assertThat((double) actualValue).as(testedFieldDescription)
                                            .isCloseTo((double) expectedValue, withPercentage(5));
                                }
                                default -> {
                                    assertThat(actualValue).as(testedFieldDescription)
                                            .isEqualTo(expectedValue);
                                }
                            }
                        }
                    }
                });
            }
            else if (isUpdate) {
                assertEquals(actualQueryResult.getUpdateCount(), expectedQueryResult.getUpdateCount());
                assertThat(expected.getUpdatedTableContent()).isPresent();
                assertThat(actual.getUpdatedTableContent()).isPresent();
                MaterializedResult expectedUpdatedTableContent = expected.getUpdatedTableContent().get();
                MaterializedResult actualUpdatedTableContent = actual.getUpdatedTableContent().get();
                assertEqualsIgnoreOrder(actualUpdatedTableContent, expectedUpdatedTableContent, "For query: \n " + query);
            }
            else if (isExplain) {
                assertEquals(actualQueryResult.getRowCount(), expectedQueryResult.getRowCount());
            }
            else {
                assertEqualsIgnoreOrder(actualQueryResult, expectedQueryResult, "For query: \n " + query);
            }

            queryAssertion.accept(actual.getQueryId());
            return this;
        }

        public FailureRecoveryAssert failsAlways(Consumer<AbstractThrowableAssert<?, ? extends Throwable>> failureAssertion)
        {
            failsWithoutRetries(failureAssertion);
            failsDespiteRetries(failureAssertion);
            return this;
        }

        public FailureRecoveryAssert failsWithoutRetries(Consumer<AbstractThrowableAssert<?, ? extends Throwable>> failureAssertion)
        {
            verifyFailureTypeAndStageSelector();
            OptionalInt failureStageId = getFailureStageId(() -> executeExpected().getQueryResult());
            failureAssertion.accept(assertThatThrownBy(() -> executeActualNoRetries(failureStageId)));
            return this;
        }

        public FailureRecoveryAssert failsDespiteRetries(Consumer<AbstractThrowableAssert<?, ? extends Throwable>> failureAssertion)
        {
            verifyFailureTypeAndStageSelector();
            OptionalInt failureStageId = getFailureStageId(() -> executeExpected().getQueryResult());
            failureAssertion.accept(assertThatThrownBy(() -> executeActual(failureStageId)));
            return this;
        }

        private void verifyFailureTypeAndStageSelector()
        {
            assertThat(failureType.isPresent() == stageSelector.isPresent()).withFailMessage("Either both or none of failureType and stageSelector must be set").isTrue();
        }

        private OptionalInt getFailureStageId(Supplier<MaterializedResult> expectedQueryResult)
        {
            if (stageSelector.isEmpty()) {
                return OptionalInt.empty();
            }
            // only compute MaterializedResult if needed
            return OptionalInt.of(stageSelector.get().apply(expectedQueryResult.get()));
        }

        private String resolveTableName(String query, String tableName)
        {
            return query.replaceAll("<table>", tableName);
        }

        private Session noRetries(Session session)
        {
            return Session.builder(session)
                    .setSystemProperty("retry_policy", "NONE")
                    .build();
        }

        private Session withTraceToken(Session session, Optional<String> traceToken)
        {
            return Session.builder(session)
                    .setTraceToken(traceToken)
                    .build();
        }
    }

    private static class ExecutionResult
    {
        private final MaterializedResult queryResult;
        private final QueryId queryId;
        private final Optional<MaterializedResult> updatedTableContent;
        private final Optional<MaterializedResult> updatedTableStatistics;

        private ExecutionResult(
                MaterializedResultWithQueryId resultWithQueryId,
                Optional<MaterializedResult> updatedTableContent,
                Optional<MaterializedResult> updatedTableStatistics)
        {
            requireNonNull(resultWithQueryId, "resultWithQueryId is null");
            this.queryResult = resultWithQueryId.getResult();
            this.queryId = resultWithQueryId.getQueryId();
            this.updatedTableContent = requireNonNull(updatedTableContent, "updatedTableContent is null");
            this.updatedTableStatistics = requireNonNull(updatedTableStatistics, "updatedTableStatistics is null");
        }

        public MaterializedResult getQueryResult()
        {
            return queryResult;
        }

        public QueryId getQueryId()
        {
            return queryId;
        }

        public Optional<MaterializedResult> getUpdatedTableContent()
        {
            return updatedTableContent;
        }

        public Optional<MaterializedResult> getUpdatedTableStatistics()
        {
            return updatedTableStatistics;
        }
    }

    protected static Function<MaterializedResult, Integer> rootStage()
    {
        return result -> parseInt(getRootStage(result).getStageId());
    }

    protected static Function<MaterializedResult, Integer> boundaryCoordinatorStage()
    {
        return result -> findStageId(result, stage -> stage.isCoordinatorOnly() && stage.getSubStages().stream().noneMatch(StageStats::isCoordinatorOnly));
    }

    protected static Function<MaterializedResult, Integer> boundaryDistributedStage()
    {
        return result -> {
            StageStats rootStage = getRootStage(result);
            if (!rootStage.isCoordinatorOnly()) {
                return parseInt(rootStage.getStageId());
            }

            StageStats boundaryCoordinatorStage = findStage(result, stage -> stage.isCoordinatorOnly() && stage.getSubStages().stream().noneMatch(StageStats::isCoordinatorOnly));
            StageStats boundaryDistributedStage = boundaryCoordinatorStage.getSubStages().get(ThreadLocalRandom.current().nextInt(boundaryCoordinatorStage.getSubStages().size()));
            return parseInt(boundaryDistributedStage.getStageId());
        };
    }

    protected static Function<MaterializedResult, Integer> intermediateDistributedStage()
    {
        return result -> findStageId(result, stage -> !stage.isCoordinatorOnly() && !stage.getSubStages().isEmpty());
    }

    protected static Function<MaterializedResult, Integer> distributedStage()
    {
        return result -> findStageId(result, stage -> !stage.isCoordinatorOnly());
    }

    protected static Function<MaterializedResult, Integer> leafStage()
    {
        return result -> findStageId(result, stage -> stage.getSubStages().isEmpty());
    }

    private static int findStageId(MaterializedResult result, Predicate<StageStats> predicate)
    {
        return parseInt(findStage(result, predicate).getStageId());
    }

    private static StageStats findStage(MaterializedResult result, Predicate<StageStats> predicate)
    {
        List<StageStats> stages = stream(Traverser.forTree(StageStats::getSubStages).breadthFirst(getRootStage(result)))
                .filter(predicate)
                .collect(toImmutableList());
        if (stages.isEmpty()) {
            throw new IllegalArgumentException("stage not found");
        }
        return stages.get(ThreadLocalRandom.current().nextInt(stages.size()));
    }

    private static StageStats getStageStats(MaterializedResult result, int stageId)
    {
        return stream(Traverser.forTree(StageStats::getSubStages).breadthFirst(getRootStage(result)))
                .filter(stageStats -> parseInt(stageStats.getStageId()) == stageId)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("stage stats not found: " + stageId));
    }

    private static StageStats getRootStage(MaterializedResult result)
    {
        StatementStats statementStats = result.getStatementStats().orElseThrow(() -> new IllegalArgumentException("statement stats is not present"));
        return requireNonNull(statementStats.getRootStage(), "root stage is null");
    }

    // The purpose of this class is two-fold:
    // 1. to allow implementations to add to the list of parallelTests
    // 2. to override the toString method so the test label is readable
    // It extends Runnable to keep the class private, while testParallel can take a Runnable
    private record ParallelTestRunnable(String name, Runnable runnable)
            implements Runnable
    {
        ParallelTestRunnable
        {
            requireNonNull(name, "name is null");
            requireNonNull(runnable, "runnable is null");
        }

        @Override
        public String toString()
        {
            return name; // so the test label is readable
        }

        @Override
        public void run()
        {
            runnable.run();
        }
    }

    protected Object[] parallelTest(String name, Runnable runnable)
    {
        return new Object[] {
                new ParallelTestRunnable(name, runnable)
        };
    }

    protected Object[][] moreParallelTests(Object[][] some, Object[]... more)
    {
        Object[][] result = Arrays.copyOf(some, some.length + more.length);
        System.arraycopy(more, 0, result, some.length, more.length);
        return result;
    }
}
