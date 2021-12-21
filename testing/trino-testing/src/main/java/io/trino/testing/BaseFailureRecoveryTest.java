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
import io.trino.spi.QueryId;
import io.trino.tpch.TpchTable;
import org.assertj.core.api.AbstractThrowableAssert;
import org.testng.annotations.BeforeClass;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;
import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.execution.FailureInjector.FAILURE_INJECTION_MESSAGE;
import static io.trino.execution.FailureInjector.InjectedFailureType.TASK_FAILURE;
import static io.trino.execution.FailureInjector.InjectedFailureType.TASK_GET_RESULTS_REQUEST_FAILURE;
import static io.trino.execution.FailureInjector.InjectedFailureType.TASK_GET_RESULTS_REQUEST_TIMEOUT;
import static io.trino.execution.FailureInjector.InjectedFailureType.TASK_MANAGEMENT_REQUEST_FAILURE;
import static io.trino.execution.FailureInjector.InjectedFailureType.TASK_MANAGEMENT_REQUEST_TIMEOUT;
import static io.trino.sql.planner.OptimizerConfig.JoinDistributionType.PARTITIONED;
import static io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy.NONE;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.SUPPLIER;
import static java.lang.Integer.parseInt;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public abstract class BaseFailureRecoveryTest
        extends AbstractTestQueryFramework
{
    protected static final int INVOCATION_COUNT = 2;
    private static final Duration MAX_ERROR_DURATION = new Duration(5, SECONDS);
    private static final Duration REQUEST_TIMEOUT = new Duration(5, SECONDS);

    private final RetryPolicy retryPolicy;

    protected BaseFailureRecoveryTest(RetryPolicy retryPolicy)
    {
        this.retryPolicy = requireNonNull(retryPolicy, "retryPolicy is null");
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
                        .put("retry-attempts", "1")
                        .put("failure-injection.request-timeout", new Duration(REQUEST_TIMEOUT.toMillis() * 2, MILLISECONDS).toString())
                        // making http timeouts shorter so tests which simulate communication timeouts finish in reasonable amount of time
                        .put("exchange.http-client.idle-timeout", REQUEST_TIMEOUT.toString())
                        .put("query.hash-partition-count", "5")
                        // to trigger spilling
                        .put("exchange.deduplication-buffer-size", "1kB")
                        .buildOrThrow(),
                ImmutableMap.<String, String>builder()
                        // making http timeouts shorter so tests which simulate communication timeouts finish in reasonable amount of time
                        .put("scheduler.http-client.idle-timeout", REQUEST_TIMEOUT.toString())
                        .buildOrThrow());
    }

    protected abstract QueryRunner createQueryRunner(
            List<TpchTable<?>> requiredTpchTables,
            Map<String, String> configProperties,
            Map<String, String> coordinatorProperties)
            throws Exception;

    @BeforeClass
    public void initTables()
            throws Exception
    {
    }

    protected abstract boolean areWriteRetriesSupported();

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
                    .failsDespiteRetries(failure -> failure.hasMessageMatching("This connector does not support query retries"));
            return;
        }

        assertThatQuery(query)
                .withSession(session)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR))
                .at(boundaryCoordinatorStage())
                .failsAlways(failure -> failure.hasMessageContaining(FAILURE_INJECTION_MESSAGE));

        assertThatQuery(query)
                .withSession(session)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR))
                .at(rootStage())
                .failsAlways(failure -> failure.hasMessageContaining(FAILURE_INJECTION_MESSAGE));

        assertThatQuery(query)
                .withSession(session)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR))
                .at(leafStage())
                .failsWithoutRetries(failure -> failure.hasMessageContaining(FAILURE_INJECTION_MESSAGE))
                .finishesSuccessfully();

        assertThatQuery(query)
                .withSession(session)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR))
                .at(boundaryDistributedStage())
                .failsWithoutRetries(failure -> failure.hasMessageContaining(FAILURE_INJECTION_MESSAGE))
                .finishesSuccessfully();

        assertThatQuery(query)
                .withSession(session)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR))
                .at(intermediateDistributedStage())
                .failsWithoutRetries(failure -> failure.hasMessageContaining(FAILURE_INJECTION_MESSAGE))
                .finishesSuccessfully();

        assertThatQuery(query)
                .withSession(session)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_MANAGEMENT_REQUEST_FAILURE)
                .at(boundaryDistributedStage())
                .failsWithoutRetries(failure -> failure.hasMessageFindingMatch("Error 500 Internal Server Error|Error closing remote buffer, expected 204 got 500"))
                .finishesSuccessfully();

        assertThatQuery(query)
                .withSession(session)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_GET_RESULTS_REQUEST_FAILURE)
                .at(boundaryDistributedStage())
                .failsWithoutRetries(failure -> failure.hasMessageFindingMatch("Error 500 Internal Server Error|Error closing remote buffer, expected 204 got 500"))
                .finishesSuccessfully();

        assertThatQuery(query)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_MANAGEMENT_REQUEST_TIMEOUT)
                .at(boundaryDistributedStage())
                .failsWithoutRetries(failure -> failure.hasMessageContaining("Encountered too many errors talking to a worker node"))
                .finishesSuccessfully();

        assertThatQuery(query)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_GET_RESULTS_REQUEST_TIMEOUT)
                .at(boundaryDistributedStage())
                .failsWithoutRetries(failure -> failure.hasMessageContaining("Encountered too many errors talking to a worker node"))
                .finishesSuccessfully();
    }

    protected FailureRecoveryAssert assertThatQuery(String query)
    {
        return new FailureRecoveryAssert(query);
    }

    protected class FailureRecoveryAssert
    {
        private final String query;
        private Session session = getQueryRunner().getDefaultSession();
        private Optional<Function<MaterializedResult, Integer>> stageSelector;
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
            String tableName = "table_" + randomTableSuffix();
            setup.ifPresent(sql -> getQueryRunner().execute(noRetries(session), resolveTableName(sql, tableName)));

            ResultWithQueryId<MaterializedResult> resultWithQueryId = null;
            RuntimeException failure = null;
            try {
                resultWithQueryId = getDistributedQueryRunner().executeWithQueryId(withTraceToken(session, traceToken), resolveTableName(query, tableName));
            }
            catch (RuntimeException e) {
                failure = e;
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

        public void finishesSuccessfully()
        {
            finishesSuccessfully(queryId -> {});
        }

        public void finishesSuccessfullyWithoutTaskFailures()
        {
            finishesSuccessfully(queryId -> {}, false);
        }

        private void finishesSuccessfully(Consumer<QueryId> queryAssertion)
        {
            finishesSuccessfully(queryAssertion, true);
        }

        public void finishesSuccessfully(Consumer<QueryId> queryAssertion, boolean expectTaskFailures)
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

            queryAssertion.accept(actual.getQueryId());
        }

        public FailureRecoveryAssert failsAlways(Consumer<AbstractThrowableAssert> failureAssertion)
        {
            failsWithoutRetries(failureAssertion);
            failsDespiteRetries(failureAssertion);
            return this;
        }

        public FailureRecoveryAssert failsWithoutRetries(Consumer<AbstractThrowableAssert> failureAssertion)
        {
            verifyFailureTypeAndStageSelector();
            OptionalInt failureStageId = getFailureStageId(() -> executeExpected().getQueryResult());
            failureAssertion.accept(assertThatThrownBy(() -> executeActualNoRetries(failureStageId)));
            return this;
        }

        public FailureRecoveryAssert failsDespiteRetries(Consumer<AbstractThrowableAssert> failureAssertion)
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
                ResultWithQueryId<MaterializedResult> resultWithQueryId,
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

    private Session enableDynamicFiltering(boolean enabled)
    {
        Session defaultSession = getQueryRunner().getDefaultSession();
        return Session.builder(defaultSession)
                .setSystemProperty(ENABLE_DYNAMIC_FILTERING, Boolean.toString(enabled))
                .setSystemProperty(JOIN_REORDERING_STRATEGY, NONE.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, PARTITIONED.name())
                .setCatalogSessionProperty(defaultSession.getCatalog().orElseThrow(), "dynamic_filtering_wait_timeout", "1h")
                .build();
    }
}
