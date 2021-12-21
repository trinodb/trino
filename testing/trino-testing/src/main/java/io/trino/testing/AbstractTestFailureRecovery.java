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
import io.trino.tpch.TpchTable;
import org.assertj.core.api.AbstractThrowableAssert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;
import static io.trino.execution.FailureInjector.FAILURE_INJECTION_MESSAGE;
import static io.trino.execution.FailureInjector.InjectedFailureType.TASK_FAILURE;
import static io.trino.execution.FailureInjector.InjectedFailureType.TASK_GET_RESULTS_REQUEST_FAILURE;
import static io.trino.execution.FailureInjector.InjectedFailureType.TASK_GET_RESULTS_REQUEST_TIMEOUT;
import static io.trino.execution.FailureInjector.InjectedFailureType.TASK_MANAGEMENT_REQUEST_FAILURE;
import static io.trino.execution.FailureInjector.InjectedFailureType.TASK_MANAGEMENT_REQUEST_TIMEOUT;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static java.lang.Integer.parseInt;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public abstract class AbstractTestFailureRecovery
        extends AbstractTestQueryFramework
{
    protected static final int INVOCATION_COUNT = 3;
    private static final Duration MAX_ERROR_DURATION = new Duration(10, SECONDS);
    private static final Duration REQUEST_TIMEOUT = new Duration(10, SECONDS);

    private final RetryPolicy retryPolicy;
    private final Map<String, String> exchangeManagerProperties;

    protected AbstractTestFailureRecovery(RetryPolicy retryPolicy, Map<String, String> exchangeManagerProperties)
    {
        this.retryPolicy = requireNonNull(retryPolicy, "retryPolicy is null");
        this.exchangeManagerProperties = requireNonNull(exchangeManagerProperties, "exchangeManagerProperties is null");
    }

    @Override
    protected final QueryRunner createQueryRunner()
            throws Exception
    {
        return createQueryRunner(
                ImmutableList.of(NATION, ORDERS, CUSTOMER),
                ImmutableMap.<String, String>builder()
                        .put("query.remote-task.max-error-duration", MAX_ERROR_DURATION.toString())
                        .put("exchange.max-error-duration", MAX_ERROR_DURATION.toString())
                        .put("retry-policy", retryPolicy.toString())
                        .put("retry-initial-delay", "0s")
                        .put("retry-attempts", "1")
                        .put("failure-injection.request-timeout", new Duration(REQUEST_TIMEOUT.toMillis() * 2, MILLISECONDS).toString())
                        .put("exchange.http-client.idle-timeout", REQUEST_TIMEOUT.toString())
                        .put("query.initial-hash-partitions", "5")
                        // TODO: re-enable once failure recover supported for this functionality
                        .put("enable-dynamic-filtering", "false")
                        .put("distributed-sort", "false")
                        .build(),
                ImmutableMap.<String, String>builder()
                        .put("scheduler.http-client.idle-timeout", REQUEST_TIMEOUT.toString())
                        .build(),
                exchangeManagerProperties);
    }

    protected abstract QueryRunner createQueryRunner(
            List<TpchTable<?>> requiredTpchTables,
            Map<String, String> configProperties,
            Map<String, String> coordinatorProperties,
            Map<String, String> exchangeManagerProperties)
            throws Exception;

    @Test(invocationCount = INVOCATION_COUNT)
    public void testSimpleSelect()
    {
        testSelect("SELECT * FROM nation");
    }

    @Test(invocationCount = INVOCATION_COUNT)
    public void testAggregation()
    {
        testSelect("SELECT orderStatus, count(*) FROM orders GROUP BY orderStatus");
    }

    @Test(invocationCount = INVOCATION_COUNT)
    public void testJoin()
    {
        testSelect("SELECT * FROM orders o, customer c WHERE o.custkey = c.custkey AND c.nationKey = 1");
    }

    protected void testSelect(String query)
    {
        assertThatQuery(query)
                .experiencing(TASK_MANAGEMENT_REQUEST_FAILURE)
                .at(leafStage())
                .finishesSuccessfully();

        assertThatQuery(query)
                .experiencing(TASK_GET_RESULTS_REQUEST_FAILURE)
                .at(boundaryDistributedStage())
                .finishesSuccessfully();

        assertThatQuery(query)
                .experiencing(TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR))
                .at(leafStage())
                .finishesSuccessfully();

        assertThatQuery(query)
                .experiencing(TASK_FAILURE, Optional.of(ErrorType.EXTERNAL))
                .at(intermediateDistributedStage())
                .finishesSuccessfully();
    }

    @Test(invocationCount = INVOCATION_COUNT)
    public void testUserFailure()
    {
        assertThatThrownBy(() -> getQueryRunner().execute("SELECT * FROM nation WHERE regionKey / nationKey - 1 = 0"))
                .hasMessageContaining("Division by zero");

        assertThatQuery("SELECT * FROM nation")
                .experiencing(TASK_FAILURE, Optional.of(ErrorType.USER_ERROR))
                .at(leafStage())
                .failsWithErrorThat()
                .hasMessageContaining(FAILURE_INJECTION_MESSAGE);
    }

    @Test(invocationCount = INVOCATION_COUNT)
    public void testCreateTable()
    {
        testTableModification(
                Optional.empty(),
                "CREATE TABLE <table> AS SELECT * FROM orders",
                Optional.of("DROP TABLE <table>"));
    }

    @Test(invocationCount = INVOCATION_COUNT)
    public void testInsert()
    {
        testTableModification(
                Optional.of("CREATE TABLE <table> AS SELECT * FROM orders WITH NO DATA"),
                "INSERT INTO <table> SELECT * FROM orders",
                Optional.of("DROP TABLE <table>"));
    }

    @Test(invocationCount = INVOCATION_COUNT)
    public void testDelete()
    {
        testTableModification(
                Optional.of("CREATE TABLE <table> AS SELECT * FROM orders"),
                "DELETE FROM orders WHERE orderkey = 1",
                Optional.of("DROP TABLE <table>"));
    }

    @Test(invocationCount = INVOCATION_COUNT)
    public void testDeleteWithSubquery()
    {
        testTableModification(
                Optional.of("CREATE TABLE <table> AS SELECT * FROM orders"),
                "DELETE FROM orders WHERE custkey IN (SELECT custkey FROM customer WHERE nationkey = 1)",
                Optional.of("DROP TABLE <table>"));
    }

    @Test(invocationCount = INVOCATION_COUNT)
    public void testUpdate()
    {
        testTableModification(
                Optional.of("CREATE TABLE <table> AS SELECT * FROM orders"),
                "UPDATE orders SET shippriority = 101 WHERE custkey = 1",
                Optional.of("DROP TABLE <table>"));
    }

    @Test(invocationCount = INVOCATION_COUNT)
    public void testUpdateWithSubquery()
    {
        testTableModification(
                Optional.of("CREATE TABLE <table> AS SELECT * FROM orders"),
                "UPDATE orders SET shippriority = 101 WHERE custkey = (SELECT min(custkey) FROM customer)",
                Optional.of("DROP TABLE <table>"));
    }

    @Test(invocationCount = INVOCATION_COUNT)
    public void testAnalyzeStatistics()
    {
        testTableModification(
                Optional.of("CREATE TABLE <table> AS SELECT * FROM orders"),
                "ANALYZE <table>",
                Optional.of("DROP TABLE <table>"));
    }

    @Test(invocationCount = INVOCATION_COUNT)
    public void testRefreshMaterializedView()
    {
        testTableModification(
                Optional.of("CREATE MATERIALIZED VIEW <table> AS SELECT * FROM orders"),
                "REFRESH MATERIALIZED VIEW <table>",
                Optional.of("DROP MATERIALIZED VIEW <table>"));
    }

    @Test(invocationCount = INVOCATION_COUNT)
    public void testExplainAnalyze()
    {
        testSelect("EXPLAIN ANALYZE SELECT orderStatus, count(*) FROM orders GROUP BY orderStatus");

        testTableModification(
                Optional.of("CREATE TABLE <table> AS SELECT * FROM orders WITH NO DATA"),
                "EXPLAIN ANALYZE INSERT INTO <table> SELECT * FROM orders",
                Optional.of("DROP TABLE <table>"));
    }

    @Test(invocationCount = INVOCATION_COUNT)
    public void testRequestTimeouts()
    {
        assertThatQuery("SELECT orderStatus, count(*) FROM orders GROUP BY orderStatus")
                .experiencing(TASK_MANAGEMENT_REQUEST_TIMEOUT)
                .at(intermediateDistributedStage())
                .finishesSuccessfully();

        assertThatQuery("SELECT * FROM nation")
                .experiencing(TASK_MANAGEMENT_REQUEST_TIMEOUT)
                .at(boundaryDistributedStage())
                .finishesSuccessfully();

        assertThatQuery("SELECT * FROM orders o, customer c WHERE o.custkey = c.custkey AND c.nationKey = 1")
                .experiencing(TASK_GET_RESULTS_REQUEST_TIMEOUT)
                .at(boundaryDistributedStage())
                .finishesSuccessfully();

        assertThatQuery("INSERT INTO <table> SELECT * FROM orders")
                .withSetupQuery(Optional.of("CREATE TABLE <table> AS SELECT * FROM orders WITH NO DATA"))
                .withCleanupQuery(Optional.of("DROP TABLE <table>"))
                .experiencing(TASK_MANAGEMENT_REQUEST_TIMEOUT)
                .at(boundaryDistributedStage())
                .finishesSuccessfully();

        assertThatQuery("INSERT INTO <table> SELECT * FROM orders")
                .withSetupQuery(Optional.of("CREATE TABLE <table> AS SELECT * FROM orders WITH NO DATA"))
                .withCleanupQuery(Optional.of("DROP TABLE <table>"))
                .experiencing(TASK_GET_RESULTS_REQUEST_TIMEOUT)
                .at(boundaryDistributedStage())
                .finishesSuccessfully();
    }

    protected void testTableModification(Optional<String> setupQuery, String query, Optional<String> cleanupQuery)
    {
        testTableModification(Optional.empty(), setupQuery, query, cleanupQuery);
    }

    protected void testTableModification(Optional<Session> session, Optional<String> setupQuery, String query, Optional<String> cleanupQuery)
    {
        assertThatQuery(query)
                .withSession(session)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR))
                .at(boundaryCoordinatorStage())
                .failsWithErrorThat()
                .hasMessageContaining(FAILURE_INJECTION_MESSAGE);

        assertThatQuery(query)
                .withSession(session)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR))
                .at(rootStage())
                .failsWithErrorThat()
                .hasMessageContaining(FAILURE_INJECTION_MESSAGE);

        assertThatQuery(query)
                .withSession(session)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR))
                .at(leafStage())
                .finishesSuccessfully();

        assertThatQuery(query)
                .withSession(session)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR))
                .at(boundaryDistributedStage())
                .finishesSuccessfully();

        assertThatQuery(query)
                .withSession(session)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR))
                .at(intermediateDistributedStage())
                .finishesSuccessfully();

        assertThatQuery(query)
                .withSession(session)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_MANAGEMENT_REQUEST_FAILURE)
                .at(boundaryDistributedStage())
                .finishesSuccessfully();

        assertThatQuery(query)
                .withSession(session)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_GET_RESULTS_REQUEST_FAILURE)
                .at(boundaryDistributedStage())
                .finishesSuccessfully();
    }

    private FailureRecoveryAssert assertThatQuery(String query)
    {
        return new FailureRecoveryAssert(query);
    }

    protected class FailureRecoveryAssert
    {
        private final String query;
        private Session session = getQueryRunner().getDefaultSession();
        private Function<MaterializedResult, Integer> stageSelector;
        private Optional<InjectedFailureType> failureType = Optional.empty();
        private Optional<ErrorType> errorType = Optional.empty();
        private Optional<String> setup = Optional.empty();
        private Optional<String> cleanup = Optional.empty();

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
            this.stageSelector = requireNonNull(stageSelector, "stageSelector is null");
            return this;
        }

        private ExecutionResult executeExpected()
        {
            return execute(query, Optional.empty());
        }

        private ExecutionResult executeActual(MaterializedResult expected)
        {
            requireNonNull(stageSelector, "stageSelector must be set");
            int stageId = stageSelector.apply(expected);
            String token = UUID.randomUUID().toString();
            failureType.ifPresent(failure -> getQueryRunner().injectTaskFailure(
                    token,
                    stageId,
                    0,
                    0,
                    failure,
                    errorType));

            ExecutionResult actual = execute(query, Optional.of(token));
            assertEquals(getStageStats(actual.getQueryResult(), stageId).getFailedTasks(), failureType.isPresent() ? 1 : 0);
            return actual;
        }

        private ExecutionResult execute(String query, Optional<String> traceToken)
        {
            String tableName = "table_" + randomTableSuffix();
            setup.ifPresent(sql -> getQueryRunner().execute(session, resolveTableName(sql, tableName)));

            MaterializedResult result = null;
            RuntimeException failure = null;
            try {
                Session sessionWithToken = Session.builder(session)
                        .setTraceToken(traceToken)
                        .build();
                result = getQueryRunner().execute(sessionWithToken, resolveTableName(query, tableName));
            }
            catch (RuntimeException e) {
                failure = e;
            }

            Optional<MaterializedResult> updatedTableContent = Optional.empty();
            if (result != null && result.getUpdateCount().isPresent()) {
                updatedTableContent = Optional.of(getQueryRunner().execute(session, "SELECT * FROM " + tableName));
            }

            Optional<MaterializedResult> updatedTableStatistics = Optional.empty();
            if (result != null && result.getUpdateType().isPresent() && result.getUpdateType().get().equals("ANALYZE")) {
                updatedTableStatistics = Optional.of(getQueryRunner().execute(session, "SHOW STATS FOR " + tableName));
            }

            try {
                cleanup.ifPresent(sql -> getQueryRunner().execute(session, resolveTableName(sql, tableName)));
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

            return new ExecutionResult(result, updatedTableContent, updatedTableStatistics);
        }

        public void finishesSuccessfully()
        {
            ExecutionResult expected = executeExpected();
            MaterializedResult expectedQueryResult = expected.getQueryResult();
            ExecutionResult actual = executeActual(expectedQueryResult);
            MaterializedResult actualQueryResult = actual.getQueryResult();

            boolean isAnalyze = expectedQueryResult.getUpdateType().isPresent() && expectedQueryResult.getUpdateType().get().equals("ANALYZE");
            boolean isUpdate = expectedQueryResult.getUpdateCount().isPresent();
            boolean isExplain = query.trim().toUpperCase(ENGLISH).startsWith("EXPLAIN");
            if (isAnalyze) {
                assertEquals(actualQueryResult.getUpdateCount(), expectedQueryResult.getUpdateCount());
                assertThat(expected.getUpdatedTableStatistics()).isPresent();
                assertThat(actual.getUpdatedTableStatistics()).isPresent();

                MaterializedResult expectedUpdatedTableStatistics = expected.getUpdatedTableStatistics().get();
                MaterializedResult actualUpdatedTableStatistics = actual.getUpdatedTableStatistics().get();
                assertEqualsIgnoreOrder(actualUpdatedTableStatistics, expectedUpdatedTableStatistics, "For query: \n " + query);
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
        }

        public AbstractThrowableAssert<?, ? extends Throwable> failsWithErrorThat()
        {
            ExecutionResult expected = executeExpected();
            return assertThatThrownBy(() -> executeActual(expected.getQueryResult()));
        }

        private String resolveTableName(String query, String tableName)
        {
            return query.replaceAll("<table>", tableName);
        }
    }

    private static class ExecutionResult
    {
        private final MaterializedResult queryResult;
        private final Optional<MaterializedResult> updatedTableContent;
        private final Optional<MaterializedResult> updatedTableStatistics;

        private ExecutionResult(
                MaterializedResult queryResult,
                Optional<MaterializedResult> updatedTableContent,
                Optional<MaterializedResult> updatedTableStatistics)
        {
            this.queryResult = requireNonNull(queryResult, "queryResult is null");
            this.updatedTableContent = requireNonNull(updatedTableContent, "updatedTableContent is null");
            this.updatedTableStatistics = requireNonNull(updatedTableStatistics, "updatedTableStatistics is null");
        }

        public MaterializedResult getQueryResult()
        {
            return queryResult;
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
        return (result) -> parseInt(getRootStage(result).getStageId());
    }

    protected static Function<MaterializedResult, Integer> boundaryCoordinatorStage()
    {
        return (result) -> findStageId(result, stage -> stage.isCoordinatorOnly() && stage.getSubStages().stream().noneMatch(StageStats::isCoordinatorOnly));
    }

    protected static Function<MaterializedResult, Integer> boundaryDistributedStage()
    {
        return (result) -> {
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
        return (result) -> findStageId(result, stage -> !stage.isCoordinatorOnly() && !stage.getSubStages().isEmpty());
    }

    protected static Function<MaterializedResult, Integer> leafStage()
    {
        return (result) -> findStageId(result, stage -> stage.getSubStages().isEmpty());
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
}
