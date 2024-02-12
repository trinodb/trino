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
package io.trino.execution;

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.testing.TestingTicker;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.Session;
import io.trino.client.FailureInfo;
import io.trino.client.NodeVersion;
import io.trino.execution.warnings.DefaultWarningCollector;
import io.trino.execution.warnings.WarningCollector;
import io.trino.execution.warnings.WarningCollectorConfig;
import io.trino.metadata.Metadata;
import io.trino.plugin.base.security.AllowAllSystemAccessControl;
import io.trino.plugin.base.security.DefaultSystemAccessControl;
import io.trino.security.AccessControlConfig;
import io.trino.security.AccessControlManager;
import io.trino.server.BasicQueryInfo;
import io.trino.server.BasicQueryStats;
import io.trino.server.ResultQueryInfo;
import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorType;
import io.trino.spi.TrinoException;
import io.trino.spi.TrinoWarning;
import io.trino.spi.WarningCode;
import io.trino.spi.connector.CatalogHandle.CatalogVersion;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.security.SelectedRole;
import io.trino.spi.type.Type;
import io.trino.sql.analyzer.Output;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.tracing.TracingMetadata;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.tracing.Tracing.noopTracer;
import static io.airlift.units.DataSize.succinctBytes;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.QueryState.DISPATCHING;
import static io.trino.execution.QueryState.FAILED;
import static io.trino.execution.QueryState.FINISHED;
import static io.trino.execution.QueryState.FINISHING;
import static io.trino.execution.QueryState.PLANNING;
import static io.trino.execution.QueryState.QUEUED;
import static io.trino.execution.QueryState.RUNNING;
import static io.trino.execution.QueryState.STARTING;
import static io.trino.execution.QueryState.WAITING_FOR_RESOURCES;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.StandardErrorCode.USER_CANCELED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingEventListenerManager.emptyEventListenerManager;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestQueryStateMachine
{
    private static final String QUERY = "sql";
    private static final URI LOCATION = URI.create("fake://fake-query");
    private static final List<Input> INPUTS = ImmutableList.of(new Input(
            "connector",
            new CatalogVersion("default"),
            "schema",
            "table",
            Optional.empty(),
            ImmutableList.of(new Column("a", "varchar")),
            new PlanFragmentId("fragment"),
            new PlanNodeId("plan-node")));
    private static final Optional<Output> OUTPUT = Optional.empty();
    private static final List<String> OUTPUT_FIELD_NAMES = ImmutableList.of("a", "b", "c");
    private static final List<Type> OUTPUT_FIELD_TYPES = ImmutableList.of(BIGINT, BIGINT, BIGINT);
    private static final String UPDATE_TYPE = "update type";
    private static final Map<String, String> SET_SESSION_PROPERTIES = ImmutableMap.<String, String>builder()
            .put("fruit", "apple")
            .put("drink", "coffee")
            .buildOrThrow();
    private static final List<String> RESET_SESSION_PROPERTIES = ImmutableList.of("candy");
    private static final Optional<QueryType> QUERY_TYPE = Optional.of(QueryType.SELECT);

    private ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "=%s"));

    @AfterAll
    public void tearDown()
    {
        executor.shutdownNow();
        executor = null;
    }

    @Test
    public void testBasicStateChanges()
    {
        QueryStateMachine stateMachine = createQueryStateMachine();
        assertState(stateMachine, QUEUED);

        assertThat(stateMachine.transitionToDispatching()).isTrue();
        assertState(stateMachine, DISPATCHING);

        assertThat(stateMachine.transitionToPlanning()).isTrue();
        assertState(stateMachine, PLANNING);

        assertThat(stateMachine.transitionToStarting()).isTrue();
        assertState(stateMachine, STARTING);

        assertThat(stateMachine.transitionToRunning()).isTrue();
        assertState(stateMachine, RUNNING);

        assertThat(stateMachine.transitionToFinishing()).isTrue();
        assertState(stateMachine, FINISHING);

        stateMachine.resultsConsumed();
        tryGetFutureValue(stateMachine.getStateChange(FINISHING), 2, SECONDS);
        assertState(stateMachine, FINISHED);
    }

    @Test
    public void testStateChangesWithResourceWaiting()
    {
        QueryStateMachine stateMachine = createQueryStateMachine();
        assertState(stateMachine, QUEUED);

        assertThat(stateMachine.transitionToWaitingForResources()).isTrue();
        assertState(stateMachine, WAITING_FOR_RESOURCES);

        assertThat(stateMachine.transitionToDispatching()).isTrue();
        assertState(stateMachine, DISPATCHING);

        assertThat(stateMachine.transitionToPlanning()).isTrue();
        assertState(stateMachine, PLANNING);

        assertThat(stateMachine.transitionToStarting()).isTrue();
        assertState(stateMachine, STARTING);

        assertThat(stateMachine.transitionToRunning()).isTrue();
        assertState(stateMachine, RUNNING);

        assertThat(stateMachine.transitionToFinishing()).isTrue();
        stateMachine.resultsConsumed();
        tryGetFutureValue(stateMachine.getStateChange(FINISHING), 2, SECONDS);
        assertState(stateMachine, FINISHED);
    }

    @Test
    public void testQueued()
    {
        // all time before the first state transition is accounted to queueing
        assertAllTimeSpentInQueueing(QUEUED, queryStateMachine -> {});
        assertAllTimeSpentInQueueing(WAITING_FOR_RESOURCES, QueryStateMachine::transitionToWaitingForResources);
        assertAllTimeSpentInQueueing(DISPATCHING, QueryStateMachine::transitionToDispatching);
        assertAllTimeSpentInQueueing(PLANNING, QueryStateMachine::transitionToPlanning);
        assertAllTimeSpentInQueueing(STARTING, QueryStateMachine::transitionToStarting);
        assertAllTimeSpentInQueueing(RUNNING, QueryStateMachine::transitionToRunning);

        assertAllTimeSpentInQueueing(FINISHED, stateMachine -> {
            stateMachine.resultsConsumed();
            stateMachine.transitionToFinishing();
            tryGetFutureValue(stateMachine.getStateChange(FINISHING), 2, SECONDS);
        });

        assertAllTimeSpentInQueueing(FAILED, stateMachine -> stateMachine.transitionToFailed(newFailedCause()));
    }

    private void assertAllTimeSpentInQueueing(QueryState expectedState, Consumer<QueryStateMachine> stateTransition)
    {
        TestingTicker ticker = new TestingTicker();
        QueryStateMachine stateMachine = queryStateMachine().withTicker(ticker).build();
        ticker.increment(7, MILLISECONDS);

        stateTransition.accept(stateMachine);
        assertThat(stateMachine.getQueryState()).isEqualTo(expectedState);

        QueryStats queryStats = stateMachine.getQueryInfo(Optional.empty()).getQueryStats();
        assertThat(queryStats.getQueuedTime()).isEqualTo(new Duration(7, MILLISECONDS));
        assertThat(queryStats.getResourceWaitingTime()).isEqualTo(new Duration(0, MILLISECONDS));
        assertThat(queryStats.getDispatchingTime()).isEqualTo(new Duration(0, MILLISECONDS));
        assertThat(queryStats.getPlanningTime()).isEqualTo(new Duration(0, MILLISECONDS));
        assertThat(queryStats.getExecutionTime()).isEqualTo(new Duration(0, MILLISECONDS));
        assertThat(queryStats.getFinishingTime()).isEqualTo(new Duration(0, MILLISECONDS));
    }

    @Test
    public void testPlanning()
    {
        QueryStateMachine stateMachine = createQueryStateMachine();
        assertThat(stateMachine.transitionToPlanning()).isTrue();
        assertState(stateMachine, PLANNING);

        assertThat(stateMachine.transitionToDispatching()).isFalse();
        assertState(stateMachine, PLANNING);

        assertThat(stateMachine.transitionToPlanning()).isFalse();
        assertState(stateMachine, PLANNING);

        assertThat(stateMachine.transitionToStarting()).isTrue();
        assertState(stateMachine, STARTING);

        stateMachine = createQueryStateMachine();
        stateMachine.transitionToPlanning();
        assertThat(stateMachine.transitionToRunning()).isTrue();
        assertState(stateMachine, RUNNING);

        stateMachine = createQueryStateMachine();
        stateMachine.transitionToPlanning();
        assertThat(stateMachine.transitionToFinishing()).isTrue();
        stateMachine.resultsConsumed();
        tryGetFutureValue(stateMachine.getStateChange(FINISHING), 2, SECONDS);
        assertState(stateMachine, FINISHED);

        stateMachine = createQueryStateMachine();
        stateMachine.transitionToPlanning();
        assertThat(stateMachine.transitionToFailed(newFailedCause())).isTrue();
        assertState(stateMachine, FAILED, newFailedCause());
    }

    @Test
    public void testStarting()
    {
        QueryStateMachine stateMachine = createQueryStateMachine();
        assertThat(stateMachine.transitionToStarting()).isTrue();
        assertState(stateMachine, STARTING);

        assertThat(stateMachine.transitionToDispatching()).isFalse();
        assertState(stateMachine, STARTING);

        assertThat(stateMachine.transitionToPlanning()).isFalse();
        assertState(stateMachine, STARTING);

        assertThat(stateMachine.transitionToStarting()).isFalse();
        assertState(stateMachine, STARTING);

        assertThat(stateMachine.transitionToRunning()).isTrue();
        assertState(stateMachine, RUNNING);

        stateMachine = createQueryStateMachine();
        stateMachine.transitionToStarting();
        stateMachine.resultsConsumed();
        assertThat(stateMachine.transitionToFinishing()).isTrue();
        tryGetFutureValue(stateMachine.getStateChange(FINISHING), 2, SECONDS);
        assertState(stateMachine, FINISHED);

        stateMachine = createQueryStateMachine();
        stateMachine.transitionToStarting();
        assertThat(stateMachine.transitionToFailed(newFailedCause())).isTrue();
        assertState(stateMachine, FAILED, newFailedCause());
    }

    @Test
    public void testRunning()
    {
        QueryStateMachine stateMachine = createQueryStateMachine();
        assertThat(stateMachine.transitionToRunning()).isTrue();
        assertState(stateMachine, RUNNING);

        assertThat(stateMachine.transitionToDispatching()).isFalse();
        assertState(stateMachine, RUNNING);

        assertThat(stateMachine.transitionToPlanning()).isFalse();
        assertState(stateMachine, RUNNING);

        assertThat(stateMachine.transitionToStarting()).isFalse();
        assertState(stateMachine, RUNNING);

        assertThat(stateMachine.transitionToRunning()).isFalse();
        assertState(stateMachine, RUNNING);

        stateMachine.resultsConsumed();
        assertThat(stateMachine.transitionToFinishing()).isTrue();
        tryGetFutureValue(stateMachine.getStateChange(FINISHING), 2, SECONDS);
        assertState(stateMachine, FINISHED);

        stateMachine = createQueryStateMachine();
        stateMachine.transitionToRunning();
        assertThat(stateMachine.transitionToFailed(newFailedCause())).isTrue();
        assertState(stateMachine, FAILED, newFailedCause());
    }

    @Test
    public void testFinished()
    {
        QueryStateMachine stateMachine = createQueryStateMachine();
        assertThat(stateMachine.transitionToFinishing()).isTrue();
        assertState(stateMachine, FINISHING);
        stateMachine.resultsConsumed();
        tryGetFutureValue(stateMachine.getStateChange(FINISHING), 2, SECONDS);
        assertFinalState(stateMachine, FINISHED);
    }

    @Test
    public void testFailed()
    {
        QueryStateMachine stateMachine = createQueryStateMachine();
        assertThat(stateMachine.transitionToFailed(newFailedCause())).isTrue();
        assertFinalState(stateMachine, FAILED, newFailedCause());
    }

    @Test
    public void testCanceled()
    {
        QueryStateMachine stateMachine = createQueryStateMachine();
        assertThat(stateMachine.transitionToCanceled()).isTrue();
        assertFinalState(stateMachine, FAILED, new TrinoException(USER_CANCELED, "canceled"));
    }

    @Test
    public void testPlanningTimeDuration()
    {
        TestingTicker mockTicker = new TestingTicker();
        QueryStateMachine stateMachine = queryStateMachine().withTicker(mockTicker).build();
        assertState(stateMachine, QUEUED);

        mockTicker.increment(25, MILLISECONDS);
        assertThat(stateMachine.transitionToWaitingForResources()).isTrue();
        assertState(stateMachine, WAITING_FOR_RESOURCES);

        mockTicker.increment(50, MILLISECONDS);
        assertThat(stateMachine.transitionToDispatching()).isTrue();
        assertState(stateMachine, DISPATCHING);

        mockTicker.increment(100, MILLISECONDS);
        assertThat(stateMachine.transitionToPlanning()).isTrue();
        assertState(stateMachine, PLANNING);

        mockTicker.increment(200, MILLISECONDS);
        assertThat(stateMachine.transitionToStarting()).isTrue();
        assertState(stateMachine, STARTING);

        mockTicker.increment(300, MILLISECONDS);
        assertThat(stateMachine.transitionToRunning()).isTrue();
        assertState(stateMachine, RUNNING);

        mockTicker.increment(400, MILLISECONDS);
        assertThat(stateMachine.transitionToFinishing()).isTrue();
        stateMachine.resultsConsumed();
        tryGetFutureValue(stateMachine.getStateChange(FINISHING), 2, SECONDS);
        assertState(stateMachine, FINISHED);

        QueryStats queryStats = stateMachine.getQueryInfo(Optional.empty()).getQueryStats();
        assertThat(queryStats.getElapsedTime().toMillis()).isEqualTo(1075);
        assertThat(queryStats.getQueuedTime().toMillis()).isEqualTo(25);
        assertThat(queryStats.getResourceWaitingTime().toMillis()).isEqualTo(50);
        assertThat(queryStats.getDispatchingTime().toMillis()).isEqualTo(100);
        assertThat(queryStats.getPlanningTime().toMillis()).isEqualTo(200);
        // there is no way to induce finishing time without a transaction and connector
        assertThat(queryStats.getFinishingTime().toMillis()).isEqualTo(0);
        // query execution time is starts when query transitions to planning
        assertThat(queryStats.getExecutionTime().toMillis()).isEqualTo(900);
    }

    @Test
    public void testUpdateMemoryUsage()
    {
        QueryStateMachine stateMachine = createQueryStateMachine();

        stateMachine.updateMemoryUsage(5, 15, 10, 1, 5, 3);
        assertThat(stateMachine.getPeakUserMemoryInBytes()).isEqualTo(5);
        assertThat(stateMachine.getPeakTotalMemoryInBytes()).isEqualTo(10);
        assertThat(stateMachine.getPeakRevocableMemoryInBytes()).isEqualTo(15);
        assertThat(stateMachine.getPeakTaskUserMemory()).isEqualTo(1);
        assertThat(stateMachine.getPeakTaskTotalMemory()).isEqualTo(3);
        assertThat(stateMachine.getPeakTaskRevocableMemory()).isEqualTo(5);

        stateMachine.updateMemoryUsage(0, 0, 0, 2, 2, 2);
        assertThat(stateMachine.getPeakUserMemoryInBytes()).isEqualTo(5);
        assertThat(stateMachine.getPeakTotalMemoryInBytes()).isEqualTo(10);
        assertThat(stateMachine.getPeakRevocableMemoryInBytes()).isEqualTo(15);
        assertThat(stateMachine.getPeakTaskUserMemory()).isEqualTo(2);
        assertThat(stateMachine.getPeakTaskTotalMemory()).isEqualTo(3);
        assertThat(stateMachine.getPeakTaskRevocableMemory()).isEqualTo(5);

        stateMachine.updateMemoryUsage(1, 1, 1, 1, 10, 5);
        assertThat(stateMachine.getPeakUserMemoryInBytes()).isEqualTo(6);
        assertThat(stateMachine.getPeakTotalMemoryInBytes()).isEqualTo(11);
        assertThat(stateMachine.getPeakRevocableMemoryInBytes()).isEqualTo(16);
        assertThat(stateMachine.getPeakTaskUserMemory()).isEqualTo(2);
        assertThat(stateMachine.getPeakTaskTotalMemory()).isEqualTo(5);
        assertThat(stateMachine.getPeakTaskRevocableMemory()).isEqualTo(10);

        stateMachine.updateMemoryUsage(3, 3, 3, 5, 1, 2);
        assertThat(stateMachine.getPeakUserMemoryInBytes()).isEqualTo(9);
        assertThat(stateMachine.getPeakTotalMemoryInBytes()).isEqualTo(14);
        assertThat(stateMachine.getPeakRevocableMemoryInBytes()).isEqualTo(19);
        assertThat(stateMachine.getPeakTaskUserMemory()).isEqualTo(5);
        assertThat(stateMachine.getPeakTaskTotalMemory()).isEqualTo(5);
        assertThat(stateMachine.getPeakTaskRevocableMemory()).isEqualTo(10);
    }

    @Test
    public void testPreserveFirstFailure()
            throws Exception
    {
        CountDownLatch cleanup = new CountDownLatch(1);
        QueryStateMachine queryStateMachine = queryStateMachine()
                .withMetadata(new TracingMetadata(noopTracer(), createTestMetadataManager())
                {
                    @Override
                    public void cleanupQuery(Session session)
                    {
                        cleanup.countDown();
                        super.cleanupQuery(session);
                    }
                })
                .build();

        Future<?> anotherThread = executor.submit(() -> {
            checkState(awaitUninterruptibly(cleanup, 10, SECONDS), "Timed out waiting for cleanup latch");
            queryStateMachine.transitionToFailed(new IllegalStateException("Second exception"));
        });
        Future<?> failingThread = executor.submit(() -> {
            queryStateMachine.transitionToFailed(new TrinoException(TYPE_MISMATCH, "First exception"));
        });

        failingThread.get(10, SECONDS);
        anotherThread.get(10, SECONDS);

        ExecutionFailureInfo failureInfo = queryStateMachine.getFinalQueryInfo().orElseThrow().getFailureInfo();
        assertThat(failureInfo).isNotNull();
        assertThat(failureInfo.getErrorCode()).isEqualTo(TYPE_MISMATCH.toErrorCode());
        assertThat(failureInfo.getMessage()).isEqualTo("First exception");

        BasicQueryInfo basicQueryInfo = queryStateMachine.getBasicQueryInfo(Optional.empty());
        assertThat(basicQueryInfo.getErrorCode()).isEqualTo(TYPE_MISMATCH.toErrorCode());
    }

    @Test
    public void testPreserveCancellation()
            throws Exception
    {
        CountDownLatch cleanup = new CountDownLatch(1);
        QueryStateMachine queryStateMachine = queryStateMachine()
                .withMetadata(new TracingMetadata(noopTracer(), createTestMetadataManager())
                {
                    @Override
                    public void cleanupQuery(Session session)
                    {
                        cleanup.countDown();
                        super.cleanupQuery(session);
                    }
                })
                .build();

        Future<?> anotherThread = executor.submit(() -> {
            checkState(awaitUninterruptibly(cleanup, 10, SECONDS), "Timed out waiting for cleanup latch");
            queryStateMachine.transitionToFailed(new IllegalStateException("Second exception"));
        });
        Future<?> cancellingThread = executor.submit(queryStateMachine::transitionToCanceled);

        cancellingThread.get(10, SECONDS);
        anotherThread.get(10, SECONDS);

        ExecutionFailureInfo failureInfo = queryStateMachine.getFinalQueryInfo().orElseThrow().getFailureInfo();
        assertThat(failureInfo).isNotNull();
        assertThat(failureInfo.getErrorCode()).isEqualTo(USER_CANCELED.toErrorCode());
        assertThat(failureInfo.getMessage()).isEqualTo("Query was canceled");

        BasicQueryInfo basicQueryInfo = queryStateMachine.getBasicQueryInfo(Optional.empty());
        assertThat(basicQueryInfo.getErrorCode()).isEqualTo(USER_CANCELED.toErrorCode());
    }

    @Test
    public void testGetResultQueryInfo()
    {
        List<TrinoWarning> trinoWarnings = ImmutableList.of(new TrinoWarning(new WarningCode(0, "name"), "message"));
        TransactionId transactionId = TransactionId.valueOf(UUID.randomUUID().toString());
        int stageCount = 4;
        int baseStatValue = 1;

        QueryStateMachine stateMachine = queryStateMachine()
                .withSetPath("path")
                .withSetCatalog("catalog")
                .withSetSchema("schema")
                .withSetRoles(ImmutableMap.of("role", SelectedRole.valueOf("NONE")))
                .withWarningCollector(new DefaultWarningCollector(new WarningCollectorConfig()))
                .withSetAuthorizationUser("user")
                .withWarnings(trinoWarnings)
                .withAddPreparedStatements(ImmutableMap.of("ps", "ps"))
                .withTransactionId(transactionId)
                .build();
        stateMachine.resultsConsumed();

        BasicStageInfo rootStage = createBasicStageInfo(stageCount, StageState.FINISHED, baseStatValue);
        ResultQueryInfo queryInfo = stateMachine.getResultQueryInfo(Optional.of(rootStage));
        BasicQueryStats stats = queryInfo.queryStats();

        assertThat(queryInfo.state()).isEqualTo(QUEUED);
        assertThat(queryInfo.scheduled()).isTrue();
        assertThat(queryInfo.updateType()).isEqualTo("update type");
        assertThat(queryInfo.finalQueryInfo()).isFalse();
        assertThat(queryInfo.errorCode()).isNull();
        assertThat(queryInfo.outputStage().get()).isEqualTo(rootStage);
        assertThat(queryInfo.failureInfo()).isNull();
        assertThat(queryInfo.setPath().get()).isEqualTo("path");
        assertThat(queryInfo.setCatalog().get()).isEqualTo("catalog");
        assertThat(queryInfo.setSchema().get()).isEqualTo("schema");
        assertThat(queryInfo.setAuthorizationUser().get()).isEqualTo("user");
        assertThat(queryInfo.resetAuthorizationUser()).isFalse();
        assertThat(queryInfo.setSessionProperties()).isEqualTo(ImmutableMap.of("drink", "coffee", "fruit", "apple"));
        assertThat(queryInfo.resetSessionProperties()).isEqualTo(ImmutableSet.of("candy"));
        assertThat(queryInfo.setRoles()).isEqualTo(ImmutableMap.of("role", SelectedRole.valueOf("NONE")));
        assertThat(queryInfo.addedPreparedStatements()).isEqualTo(ImmutableMap.of("ps", "ps"));
        assertThat(queryInfo.deallocatedPreparedStatements()).isEmpty();
        assertThat(queryInfo.startedTransactionId().get()).isEqualTo(transactionId);
        assertThat(queryInfo.clearTransactionId()).isFalse();
        assertThat(queryInfo.warnings()).isEqualTo(trinoWarnings);

        assertStats(stats, baseStatValue * stageCount);

        stateMachine.transitionToFailed(new TrinoException(() -> new ErrorCode(0, "", ErrorType.EXTERNAL), "", new IOException()));
        queryInfo = stateMachine.getResultQueryInfo(Optional.of(rootStage));
        assertThat(queryInfo.failureInfo()).isNotNull();
        assertThat(queryInfo.errorCode().getCode()).isEqualTo(0);
        assertThat(queryInfo.finalQueryInfo()).isTrue();
        assertThat(queryInfo.state()).isEqualTo(FAILED);
    }

    private void assertStats(BasicQueryStats stats, int expectedStatsValue)
    {
        assertThat(stats.getCreateTime()).isNotNull();
        assertThat(stats.getEndTime()).isNull();
        assertThat(stats.getQueuedTime()).isNotNull();
        assertThat(stats.getElapsedTime()).isNotNull();
        assertThat(stats.getExecutionTime()).isNotNull();
        assertThat(stats.getFailedTasks()).isEqualTo(expectedStatsValue);
        assertThat(stats.getTotalDrivers()).isEqualTo(expectedStatsValue);
        assertThat(stats.getQueuedDrivers()).isEqualTo(expectedStatsValue);
        assertThat(stats.getRunningDrivers()).isEqualTo(expectedStatsValue);
        assertThat(stats.getCompletedDrivers()).isEqualTo(expectedStatsValue);
        assertThat(stats.getBlockedDrivers()).isEqualTo(expectedStatsValue);
        assertThat(stats.getRawInputDataSize()).isEqualTo(succinctBytes(expectedStatsValue));
        assertThat(stats.getRawInputPositions()).isEqualTo(expectedStatsValue);
        assertThat(stats.getPhysicalInputDataSize()).isEqualTo(succinctBytes(expectedStatsValue));
        assertThat(stats.getPhysicalWrittenDataSize()).isEqualTo(succinctBytes(expectedStatsValue));
        assertThat(stats.getSpilledDataSize()).isEqualTo(succinctBytes(expectedStatsValue));
        assertThat(stats.getCumulativeUserMemory()).isEqualTo(expectedStatsValue);
        assertThat(stats.getFailedCumulativeUserMemory()).isEqualTo(expectedStatsValue);
        assertThat(stats.getUserMemoryReservation()).isEqualTo(succinctBytes(expectedStatsValue));
        assertThat(stats.getTotalMemoryReservation()).isEqualTo(succinctBytes(expectedStatsValue));
        assertThat(stats.getPeakTotalMemoryReservation()).isEqualTo(succinctBytes(0));
        assertThat(stats.getPeakUserMemoryReservation()).isEqualTo(succinctBytes(0));
        assertThat(stats.getTotalCpuTime()).isEqualTo(Duration.succinctDuration(expectedStatsValue, SECONDS));
        assertThat(stats.getFailedCpuTime()).isEqualTo(Duration.succinctDuration(expectedStatsValue, SECONDS));
        assertThat(stats.getTotalScheduledTime()).isEqualTo(Duration.succinctDuration(expectedStatsValue, SECONDS));
        assertThat(stats.getFailedScheduledTime()).isEqualTo(Duration.succinctDuration(expectedStatsValue, SECONDS));
        assertThat(stats.isFullyBlocked()).isFalse();
        assertThat(stats.getBlockedReasons()).isEmpty();
        assertThat(stats.getProgressPercentage()).isEmpty();
        assertThat(stats.getRunningPercentage()).isEmpty();
    }

    private BasicStageInfo createBasicStageInfo(int count, StageState state, int baseValue)
    {
        return new BasicStageInfo(
                StageId.valueOf(ImmutableList.of("s", String.valueOf(count))),
                state,
                false,
                createBasicStageStats(baseValue),
                count == 1 ? ImmutableList.of() : ImmutableList.of(createBasicStageInfo(count - 1, state, baseValue)),
                ImmutableList.of());
    }

    private BasicStageStats createBasicStageStats(int value)
    {
        return new BasicStageStats(
                false,
                value,
                value,
                value,
                value,
                value,
                value,
                DataSize.of(value, DataSize.Unit.BYTE),
                value,
                Duration.succinctDuration(value, SECONDS),
                DataSize.of(value, DataSize.Unit.BYTE),
                DataSize.of(value, DataSize.Unit.BYTE),
                value,
                DataSize.of(value, DataSize.Unit.BYTE),
                value,
                DataSize.of(value, DataSize.Unit.BYTE),
                value,
                value,
                DataSize.of(value, DataSize.Unit.BYTE),
                succinctBytes(value),
                Duration.succinctDuration(value, SECONDS),
                Duration.succinctDuration(value, SECONDS),
                Duration.succinctDuration(value, SECONDS),
                Duration.succinctDuration(value, SECONDS),
                false,
                ImmutableSet.of(),
                OptionalDouble.of(value),
                OptionalDouble.of(value));
    }

    private static void assertFinalState(QueryStateMachine stateMachine, QueryState expectedState)
    {
        assertFinalState(stateMachine, expectedState, null);
    }

    private static void assertFinalState(QueryStateMachine stateMachine, QueryState expectedState, Exception expectedException)
    {
        assertThat(expectedState.isDone()).isTrue();
        assertState(stateMachine, expectedState, expectedException);

        assertThat(stateMachine.transitionToDispatching()).isFalse();
        assertState(stateMachine, expectedState, expectedException);

        assertThat(stateMachine.transitionToPlanning()).isFalse();
        assertState(stateMachine, expectedState, expectedException);

        assertThat(stateMachine.transitionToStarting()).isFalse();
        assertState(stateMachine, expectedState, expectedException);

        assertThat(stateMachine.transitionToRunning()).isFalse();
        assertState(stateMachine, expectedState, expectedException);

        assertThat(stateMachine.transitionToFinishing()).isFalse();
        assertState(stateMachine, expectedState, expectedException);

        assertThat(stateMachine.transitionToFailed(newFailedCause())).isFalse();
        assertState(stateMachine, expectedState, expectedException);

        // attempt to fail with another exception, which will fail
        assertThat(stateMachine.transitionToFailed(new IOException("failure after finish"))).isFalse();
        assertState(stateMachine, expectedState, expectedException);
    }

    private static void assertState(QueryStateMachine stateMachine, QueryState expectedState)
    {
        assertState(stateMachine, expectedState, null);
    }

    private static void assertState(QueryStateMachine stateMachine, QueryState expectedState, Exception expectedException)
    {
        assertThat(stateMachine.getQueryId()).isEqualTo(TEST_SESSION.getQueryId());
        assertEqualSessionsWithoutTransactionId(stateMachine.getSession(), TEST_SESSION);
        assertThat(stateMachine.getSetSessionProperties()).isEqualTo(SET_SESSION_PROPERTIES);
        assertThat(stateMachine.getResetSessionProperties()).containsExactlyElementsOf(RESET_SESSION_PROPERTIES);

        QueryInfo queryInfo = stateMachine.getQueryInfo(Optional.empty());
        assertThat(queryInfo.getQueryId()).isEqualTo(TEST_SESSION.getQueryId());
        assertThat(queryInfo.getSelf()).isEqualTo(LOCATION);
        assertThat(queryInfo.getOutputStage().isPresent()).isFalse();
        assertThat(queryInfo.getQuery()).isEqualTo(QUERY);
        assertThat(queryInfo.getInputs()).containsExactlyElementsOf(INPUTS);
        assertThat(queryInfo.getOutput()).isEqualTo(OUTPUT);
        assertThat(queryInfo.getFieldNames()).containsExactlyElementsOf(OUTPUT_FIELD_NAMES);
        assertThat(queryInfo.getUpdateType()).isEqualTo(UPDATE_TYPE);
        assertThat(queryInfo.getQueryType().isPresent()).isTrue();
        assertThat(queryInfo.getQueryType().get()).isEqualTo(QUERY_TYPE.get());

        QueryStats queryStats = queryInfo.getQueryStats();
        assertThat(queryStats.getElapsedTime()).isNotNull();
        assertThat(queryStats.getQueuedTime()).isNotNull();
        assertThat(queryStats.getResourceWaitingTime()).isNotNull();
        assertThat(queryStats.getDispatchingTime()).isNotNull();
        assertThat(queryStats.getExecutionTime()).isNotNull();
        assertThat(queryStats.getPlanningTime()).isNotNull();
        assertThat(queryStats.getPlanningCpuTime()).isNotNull();
        assertThat(queryStats.getFinishingTime()).isNotNull();

        assertThat(queryStats.getCreateTime()).isNotNull();
        if (queryInfo.getState() == QUEUED || queryInfo.getState() == WAITING_FOR_RESOURCES || queryInfo.getState() == DISPATCHING) {
            assertThat(queryStats.getExecutionStartTime()).isNull();
        }
        else {
            assertThat(queryStats.getExecutionStartTime()).isNotNull();
        }
        if (queryInfo.getState().isDone()) {
            assertThat(queryStats.getEndTime()).isNotNull();
        }
        else {
            assertThat(queryStats.getEndTime()).isNull();
        }

        assertThat(stateMachine.getQueryState()).isEqualTo(expectedState);
        assertThat(queryInfo.getState()).isEqualTo(expectedState);
        assertThat(stateMachine.isDone()).isEqualTo(expectedState.isDone());

        if (expectedState == FAILED) {
            assertThat(queryInfo.getFailureInfo()).isNotNull();
            FailureInfo failure = queryInfo.getFailureInfo().toFailureInfo();
            assertThat(failure).isNotNull();
            assertThat(failure.getType()).isEqualTo(expectedException.getClass().getName());
            if (expectedException instanceof TrinoException) {
                assertThat(queryInfo.getErrorCode()).isEqualTo(((TrinoException) expectedException).getErrorCode());
            }
            else {
                assertThat(queryInfo.getErrorCode()).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode());
            }
        }
        else {
            assertThat(queryInfo.getFailureInfo()).isNull();
        }
    }

    private QueryStateMachine createQueryStateMachine()
    {
        return queryStateMachine().build();
    }

    private QueryStateMachineBuilder queryStateMachine()
    {
        return new QueryStateMachineBuilder();
    }

    private class QueryStateMachineBuilder
    {
        private Ticker ticker = Ticker.systemTicker();
        private Metadata metadata;
        private WarningCollector warningCollector = WarningCollector.NOOP;
        private String setCatalog;
        private String setPath;
        private String setSchema;
        private Map<String, SelectedRole> setRoles = ImmutableMap.of();
        private List<TrinoWarning> warnings = ImmutableList.of();
        private String setAuthorizationUser;
        private TransactionId transactionId;
        private ImmutableMap<String, String> addPreparedStatements = ImmutableMap.of();

        @CanIgnoreReturnValue
        public QueryStateMachineBuilder withTicker(Ticker ticker)
        {
            this.ticker = ticker;
            return this;
        }

        @CanIgnoreReturnValue
        public QueryStateMachineBuilder withMetadata(Metadata metadata)
        {
            this.metadata = metadata;
            return this;
        }

        public QueryStateMachineBuilder withWarningCollector(WarningCollector warningCollector)
        {
            this.warningCollector = warningCollector;
            return this;
        }

        public QueryStateMachineBuilder withSetPath(String setPath)
        {
            this.setPath = setPath;
            return this;
        }

        public QueryStateMachineBuilder withSetCatalog(String setCatalog)
        {
            this.setCatalog = setCatalog;
            return this;
        }

        public QueryStateMachineBuilder withSetSchema(String setSchema)
        {
            this.setSchema = setSchema;
            return this;
        }

        public QueryStateMachineBuilder withSetAuthorizationUser(String setAuthorizationUser)
        {
            this.setAuthorizationUser = setAuthorizationUser;
            return this;
        }

        public QueryStateMachineBuilder withSetRoles(Map<String, SelectedRole> setRoles)
        {
            this.setRoles = ImmutableMap.copyOf(setRoles);
            return this;
        }

        public QueryStateMachineBuilder withWarnings(List<TrinoWarning> warnings)
        {
            this.warnings = ImmutableList.copyOf(warnings);
            return this;
        }

        public QueryStateMachineBuilder withTransactionId(TransactionId transactionId)
        {
            this.transactionId = transactionId;
            return this;
        }

        public QueryStateMachineBuilder withAddPreparedStatements(Map<String, String> preparedStatements)
        {
            this.addPreparedStatements = ImmutableMap.copyOf(preparedStatements);
            return this;
        }

        public QueryStateMachine build()
        {
            if (metadata == null) {
                metadata = createTestMetadataManager();
            }
            TransactionManager transactionManager = createTestTransactionManager();
            AccessControlManager accessControl = new AccessControlManager(
                    NodeVersion.UNKNOWN,
                    transactionManager,
                    emptyEventListenerManager(),
                    new AccessControlConfig(),
                    OpenTelemetry.noop(),
                    new SecretsResolver(ImmutableMap.of()),
                    DefaultSystemAccessControl.NAME);
            accessControl.setSystemAccessControls(List.of(AllowAllSystemAccessControl.INSTANCE));
            QueryStateMachine stateMachine = QueryStateMachine.beginWithTicker(
                    Optional.empty(),
                    QUERY,
                    Optional.empty(),
                    TEST_SESSION,
                    LOCATION,
                    new ResourceGroupId("test"),
                    false,
                    transactionManager,
                    accessControl,
                    executor,
                    ticker,
                    metadata,
                    warningCollector,
                    createPlanOptimizersStatsCollector(),
                    QUERY_TYPE,
                    false,
                    new NodeVersion("test"));
            stateMachine.setInputs(INPUTS);
            stateMachine.setOutput(OUTPUT);
            stateMachine.setColumns(OUTPUT_FIELD_NAMES, OUTPUT_FIELD_TYPES);
            if (setPath != null) {
                stateMachine.setSetPath(setPath);
            }
            if (setCatalog != null) {
                stateMachine.setSetCatalog(setCatalog);
            }
            if (setSchema != null) {
                stateMachine.setSetSchema(setSchema);
            }
            if (setAuthorizationUser != null) {
                stateMachine.setSetAuthorizationUser(setAuthorizationUser);
            }
            addPreparedStatements.forEach(stateMachine::addPreparedStatement);
            if (transactionId != null) {
                stateMachine.setStartedTransactionId(transactionId);
            }
            setRoles.forEach(stateMachine::addSetRole);
            stateMachine.setUpdateType(UPDATE_TYPE);
            for (Entry<String, String> entry : SET_SESSION_PROPERTIES.entrySet()) {
                stateMachine.addSetSessionProperties(entry.getKey(), entry.getValue());
            }
            warnings.forEach(warning -> stateMachine.getWarningCollector().add(warning));
            RESET_SESSION_PROPERTIES.forEach(stateMachine::addResetSessionProperties);
            return stateMachine;
        }
    }

    private static void assertEqualSessionsWithoutTransactionId(Session actual, Session expected)
    {
        assertThat(actual.getQueryId()).isEqualTo(expected.getQueryId());
        assertThat(actual.getIdentity()).isEqualTo(expected.getIdentity());
        assertThat(actual.getSource()).isEqualTo(expected.getSource());
        assertThat(actual.getCatalog()).isEqualTo(expected.getCatalog());
        assertThat(actual.getSchema()).isEqualTo(expected.getSchema());
        assertThat(actual.getTimeZoneKey()).isEqualTo(expected.getTimeZoneKey());
        assertThat(actual.getLocale()).isEqualTo(expected.getLocale());
        assertThat(actual.getRemoteUserAddress()).isEqualTo(expected.getRemoteUserAddress());
        assertThat(actual.getUserAgent()).isEqualTo(expected.getUserAgent());
        assertThat(actual.getStart()).isEqualTo(expected.getStart());
        assertThat(actual.getSystemProperties()).isEqualTo(expected.getSystemProperties());
        assertThat(actual.getCatalogProperties()).isEqualTo(expected.getCatalogProperties());
    }

    private static SQLException newFailedCause()
    {
        return new SQLException("FAILED");
    }
}
