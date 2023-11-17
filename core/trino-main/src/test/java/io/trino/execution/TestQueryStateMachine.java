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
import io.airlift.testing.TestingTicker;
import io.airlift.units.Duration;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.Session;
import io.trino.client.FailureInfo;
import io.trino.client.NodeVersion;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.plugin.base.security.AllowAllSystemAccessControl;
import io.trino.plugin.base.security.DefaultSystemAccessControl;
import io.trino.security.AccessControlConfig;
import io.trino.security.AccessControlManager;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle.CatalogVersion;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.type.Type;
import io.trino.sql.analyzer.Output;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
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
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
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
        QueryStateMachine stateMachine = createQueryStateMachineWithTicker(ticker);
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
        QueryStateMachine stateMachine = createQueryStateMachineWithTicker(mockTicker);
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
        return createQueryStateMachineWithTicker(Ticker.systemTicker());
    }

    private QueryStateMachine createQueryStateMachineWithTicker(Ticker ticker)
    {
        Metadata metadata = createTestMetadataManager();
        TransactionManager transactionManager = createTestTransactionManager();
        AccessControlManager accessControl = new AccessControlManager(
                NodeVersion.UNKNOWN,
                transactionManager,
                emptyEventListenerManager(),
                new AccessControlConfig(),
                OpenTelemetry.noop(),
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
                WarningCollector.NOOP,
                createPlanOptimizersStatsCollector(),
                QUERY_TYPE,
                true,
                new NodeVersion("test"));
        stateMachine.setInputs(INPUTS);
        stateMachine.setOutput(OUTPUT);
        stateMachine.setColumns(OUTPUT_FIELD_NAMES, OUTPUT_FIELD_TYPES);
        stateMachine.setUpdateType(UPDATE_TYPE);
        for (Entry<String, String> entry : SET_SESSION_PROPERTIES.entrySet()) {
            stateMachine.addSetSessionProperties(entry.getKey(), entry.getValue());
        }
        RESET_SESSION_PROPERTIES.forEach(stateMachine::addResetSessionProperties);
        return stateMachine;
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
