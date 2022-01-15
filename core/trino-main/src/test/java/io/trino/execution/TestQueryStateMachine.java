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
import io.airlift.testing.TestingTicker;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.client.FailureInfo;
import io.trino.execution.warnings.WarningCollector;
import io.trino.memory.VersionedMemoryPoolId;
import io.trino.metadata.Metadata;
import io.trino.plugin.base.security.AllowAllSystemAccessControl;
import io.trino.plugin.base.security.DefaultSystemAccessControl;
import io.trino.security.AccessControlConfig;
import io.trino.security.AccessControlManager;
import io.trino.spi.TrinoException;
import io.trino.spi.eventlistener.StageGcStatistics;
import io.trino.spi.memory.MemoryPoolId;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.type.Type;
import io.trino.sql.analyzer.Output;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.transaction.TransactionManager;
import org.joda.time.DateTime;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
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
import static io.trino.execution.TestStageStats.getTestDistribution;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.USER_CANCELED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingEventListenerManager.emptyEventListenerManager;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class TestQueryStateMachine
{
    private static final String QUERY = "sql";
    private static final URI LOCATION = URI.create("fake://fake-query");
    private static final SQLException FAILED_CAUSE = new SQLException("FAILED");
    private static final List<Input> INPUTS = ImmutableList.of(new Input(
            "connector",
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
    private static final VersionedMemoryPoolId MEMORY_POOL = new VersionedMemoryPoolId(new MemoryPoolId("pool"), 42);
    private static final Map<String, String> SET_SESSION_PROPERTIES = ImmutableMap.<String, String>builder()
            .put("fruit", "apple")
            .put("drink", "coffee")
            .build();
    private static final List<String> RESET_SESSION_PROPERTIES = ImmutableList.of("candy");
    private static final Optional<QueryType> QUERY_TYPE = Optional.of(QueryType.SELECT);

    private ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "=%s"));

    @AfterClass(alwaysRun = true)
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

        assertTrue(stateMachine.transitionToDispatching());
        assertState(stateMachine, DISPATCHING);

        assertTrue(stateMachine.transitionToPlanning());
        assertState(stateMachine, PLANNING);

        assertTrue(stateMachine.transitionToStarting());
        assertState(stateMachine, STARTING);

        assertTrue(stateMachine.transitionToRunning());
        assertState(stateMachine, RUNNING);

        assertTrue(stateMachine.transitionToFinishing());
        tryGetFutureValue(stateMachine.getStateChange(FINISHING), 2, SECONDS);
        assertState(stateMachine, FINISHED);
    }

    @Test
    public void testStateChangesWithResourceWaiting()
    {
        QueryStateMachine stateMachine = createQueryStateMachine();
        assertState(stateMachine, QUEUED);

        assertTrue(stateMachine.transitionToWaitingForResources());
        assertState(stateMachine, WAITING_FOR_RESOURCES);

        assertTrue(stateMachine.transitionToDispatching());
        assertState(stateMachine, DISPATCHING);

        assertTrue(stateMachine.transitionToPlanning());
        assertState(stateMachine, PLANNING);

        assertTrue(stateMachine.transitionToStarting());
        assertState(stateMachine, STARTING);

        assertTrue(stateMachine.transitionToRunning());
        assertState(stateMachine, RUNNING);

        assertTrue(stateMachine.transitionToFinishing());
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
            stateMachine.transitionToFinishing();
            tryGetFutureValue(stateMachine.getStateChange(FINISHING), 2, SECONDS);
        });

        assertAllTimeSpentInQueueing(FAILED, stateMachine -> stateMachine.transitionToFailed(FAILED_CAUSE));
    }

    private void assertAllTimeSpentInQueueing(QueryState expectedState, Consumer<QueryStateMachine> stateTransition)
    {
        TestingTicker ticker = new TestingTicker();
        QueryStateMachine stateMachine = createQueryStateMachineWithTicker(ticker);
        ticker.increment(7, MILLISECONDS);

        stateTransition.accept(stateMachine);
        assertEquals(stateMachine.getQueryState(), expectedState);

        QueryStats queryStats = stateMachine.getQueryInfo(Optional.empty()).getQueryStats();
        assertEquals(queryStats.getQueuedTime(), new Duration(7, MILLISECONDS));
        assertEquals(queryStats.getResourceWaitingTime(), new Duration(0, MILLISECONDS));
        assertEquals(queryStats.getDispatchingTime(), new Duration(0, MILLISECONDS));
        assertEquals(queryStats.getPlanningTime(), new Duration(0, MILLISECONDS));
        assertEquals(queryStats.getExecutionTime(), new Duration(0, MILLISECONDS));
        assertEquals(queryStats.getFinishingTime(), new Duration(0, MILLISECONDS));
    }

    @Test
    public void testPlanning()
    {
        QueryStateMachine stateMachine = createQueryStateMachine();
        assertTrue(stateMachine.transitionToPlanning());
        assertState(stateMachine, PLANNING);

        assertFalse(stateMachine.transitionToDispatching());
        assertState(stateMachine, PLANNING);

        assertFalse(stateMachine.transitionToPlanning());
        assertState(stateMachine, PLANNING);

        assertTrue(stateMachine.transitionToStarting());
        assertState(stateMachine, STARTING);

        stateMachine = createQueryStateMachine();
        stateMachine.transitionToPlanning();
        assertTrue(stateMachine.transitionToRunning());
        assertState(stateMachine, RUNNING);

        stateMachine = createQueryStateMachine();
        stateMachine.transitionToPlanning();
        assertTrue(stateMachine.transitionToFinishing());
        tryGetFutureValue(stateMachine.getStateChange(FINISHING), 2, SECONDS);
        assertState(stateMachine, FINISHED);

        stateMachine = createQueryStateMachine();
        stateMachine.transitionToPlanning();
        assertTrue(stateMachine.transitionToFailed(FAILED_CAUSE));
        assertState(stateMachine, FAILED, FAILED_CAUSE);
    }

    @Test
    public void testStarting()
    {
        QueryStateMachine stateMachine = createQueryStateMachine();
        assertTrue(stateMachine.transitionToStarting());
        assertState(stateMachine, STARTING);

        assertFalse(stateMachine.transitionToDispatching());
        assertState(stateMachine, STARTING);

        assertFalse(stateMachine.transitionToPlanning());
        assertState(stateMachine, STARTING);

        assertFalse(stateMachine.transitionToStarting());
        assertState(stateMachine, STARTING);

        assertTrue(stateMachine.transitionToRunning());
        assertState(stateMachine, RUNNING);

        stateMachine = createQueryStateMachine();
        stateMachine.transitionToStarting();
        assertTrue(stateMachine.transitionToFinishing());
        tryGetFutureValue(stateMachine.getStateChange(FINISHING), 2, SECONDS);
        assertState(stateMachine, FINISHED);

        stateMachine = createQueryStateMachine();
        stateMachine.transitionToStarting();
        assertTrue(stateMachine.transitionToFailed(FAILED_CAUSE));
        assertState(stateMachine, FAILED, FAILED_CAUSE);
    }

    @Test
    public void testRunning()
    {
        QueryStateMachine stateMachine = createQueryStateMachine();
        assertTrue(stateMachine.transitionToRunning());
        assertState(stateMachine, RUNNING);

        assertFalse(stateMachine.transitionToDispatching());
        assertState(stateMachine, RUNNING);

        assertFalse(stateMachine.transitionToPlanning());
        assertState(stateMachine, RUNNING);

        assertFalse(stateMachine.transitionToStarting());
        assertState(stateMachine, RUNNING);

        assertFalse(stateMachine.transitionToRunning());
        assertState(stateMachine, RUNNING);

        assertTrue(stateMachine.transitionToFinishing());
        tryGetFutureValue(stateMachine.getStateChange(FINISHING), 2, SECONDS);
        assertState(stateMachine, FINISHED);

        stateMachine = createQueryStateMachine();
        stateMachine.transitionToRunning();
        assertTrue(stateMachine.transitionToFailed(FAILED_CAUSE));
        assertState(stateMachine, FAILED, FAILED_CAUSE);
    }

    @Test
    public void testFinished()
    {
        QueryStateMachine stateMachine = createQueryStateMachine();
        assertTrue(stateMachine.transitionToFinishing());
        tryGetFutureValue(stateMachine.getStateChange(FINISHING), 2, SECONDS);
        assertFinalState(stateMachine, FINISHED);
    }

    @Test
    public void testFailed()
    {
        QueryStateMachine stateMachine = createQueryStateMachine();
        assertTrue(stateMachine.transitionToFailed(FAILED_CAUSE));
        assertFinalState(stateMachine, FAILED, FAILED_CAUSE);
    }

    @Test
    public void testCanceled()
    {
        QueryStateMachine stateMachine = createQueryStateMachine();
        assertTrue(stateMachine.transitionToCanceled());
        assertFinalState(stateMachine, FAILED, new TrinoException(USER_CANCELED, "canceled"));
    }

    @Test
    public void testPlanningTimeDuration()
    {
        TestingTicker mockTicker = new TestingTicker();
        QueryStateMachine stateMachine = createQueryStateMachineWithTicker(mockTicker);
        assertState(stateMachine, QUEUED);

        mockTicker.increment(25, MILLISECONDS);
        assertTrue(stateMachine.transitionToWaitingForResources());
        assertState(stateMachine, WAITING_FOR_RESOURCES);

        mockTicker.increment(50, MILLISECONDS);
        assertTrue(stateMachine.transitionToDispatching());
        assertState(stateMachine, DISPATCHING);

        mockTicker.increment(100, MILLISECONDS);
        assertTrue(stateMachine.transitionToPlanning());
        assertState(stateMachine, PLANNING);

        mockTicker.increment(200, MILLISECONDS);
        assertTrue(stateMachine.transitionToStarting());
        assertState(stateMachine, STARTING);

        mockTicker.increment(300, MILLISECONDS);
        assertTrue(stateMachine.transitionToRunning());
        assertState(stateMachine, RUNNING);

        mockTicker.increment(400, MILLISECONDS);
        assertTrue(stateMachine.transitionToFinishing());
        tryGetFutureValue(stateMachine.getStateChange(FINISHING), 2, SECONDS);
        assertState(stateMachine, FINISHED);

        QueryStats queryStats = stateMachine.getQueryInfo(Optional.empty()).getQueryStats();
        assertEquals(queryStats.getElapsedTime().toMillis(), 1075);
        assertEquals(queryStats.getQueuedTime().toMillis(), 25);
        assertEquals(queryStats.getResourceWaitingTime().toMillis(), 50);
        assertEquals(queryStats.getDispatchingTime().toMillis(), 100);
        assertEquals(queryStats.getPlanningTime().toMillis(), 200);
        // there is no way to induce finishing time without a transaction and connector
        assertEquals(queryStats.getFinishingTime().toMillis(), 0);
        // query execution time is starts when query transitions to planning
        assertEquals(queryStats.getExecutionTime().toMillis(), 900);
    }

    @Test
    public void testUpdateMemoryUsage()
    {
        QueryStateMachine stateMachine = createQueryStateMachine();

        stateMachine.updateMemoryUsage(5, 15, 10, 1, 5, 3);
        assertEquals(stateMachine.getPeakUserMemoryInBytes(), 5);
        assertEquals(stateMachine.getPeakTotalMemoryInBytes(), 10);
        assertEquals(stateMachine.getPeakRevocableMemoryInBytes(), 15);
        assertEquals(stateMachine.getPeakTaskUserMemory(), 1);
        assertEquals(stateMachine.getPeakTaskTotalMemory(), 3);
        assertEquals(stateMachine.getPeakTaskRevocableMemory(), 5);

        stateMachine.updateMemoryUsage(0, 0, 0, 2, 2, 2);
        assertEquals(stateMachine.getPeakUserMemoryInBytes(), 5);
        assertEquals(stateMachine.getPeakTotalMemoryInBytes(), 10);
        assertEquals(stateMachine.getPeakRevocableMemoryInBytes(), 15);
        assertEquals(stateMachine.getPeakTaskUserMemory(), 2);
        assertEquals(stateMachine.getPeakTaskTotalMemory(), 3);
        assertEquals(stateMachine.getPeakTaskRevocableMemory(), 5);

        stateMachine.updateMemoryUsage(1, 1, 1, 1, 10, 5);
        assertEquals(stateMachine.getPeakUserMemoryInBytes(), 6);
        assertEquals(stateMachine.getPeakTotalMemoryInBytes(), 11);
        assertEquals(stateMachine.getPeakRevocableMemoryInBytes(), 16);
        assertEquals(stateMachine.getPeakTaskUserMemory(), 2);
        assertEquals(stateMachine.getPeakTaskTotalMemory(), 5);
        assertEquals(stateMachine.getPeakTaskRevocableMemory(), 10);

        stateMachine.updateMemoryUsage(3, 3, 3, 5, 1, 2);
        assertEquals(stateMachine.getPeakUserMemoryInBytes(), 9);
        assertEquals(stateMachine.getPeakTotalMemoryInBytes(), 14);
        assertEquals(stateMachine.getPeakRevocableMemoryInBytes(), 19);
        assertEquals(stateMachine.getPeakTaskUserMemory(), 5);
        assertEquals(stateMachine.getPeakTaskTotalMemory(), 5);
        assertEquals(stateMachine.getPeakTaskRevocableMemory(), 10);
    }

    @Test
    public void testInputDataStat()
    {
        QueryStateMachine stateMachine = createQueryStateMachine();

        StageStats stageStats = new StageStats(
                new DateTime(0),
                getTestDistribution(0),
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                new Duration(0, NANOSECONDS),
                new Duration(0, NANOSECONDS),
                new Duration(0, NANOSECONDS),
                false,
                ImmutableSet.of(),
                DataSize.ofBytes(0),
                0,
                new Duration(0, NANOSECONDS),
                DataSize.ofBytes(0),
                0,

                DataSize.ofBytes(20),
                DataSize.ofBytes(10),
                25,
                10,

                DataSize.ofBytes(30),
                DataSize.ofBytes(10),
                35,
                10,

                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                0,
                DataSize.ofBytes(0),
                new StageGcStatistics(
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0),
                ImmutableList.of());

        StageInfo stageInfoMock = new StageInfo(
                new StageId("test", 1),
                StageState.FINISHED,
                TaskTestUtils.PLAN_FRAGMENT,
                false,
                new ArrayList<>(),
                stageStats,
                new ArrayList<>(),
                new ArrayList<>(),
                new HashMap<>(),
                null);
        QueryStats queryStats = stateMachine.getQueryInfo(Optional.of(stageInfoMock)).getQueryStats();

        assertEquals(queryStats.getRawInputDataSize().toBytes(), 10);
        assertEquals(queryStats.getRawInputPositions(), 15);
        assertEquals(queryStats.getProcessedInputDataSize().toBytes(), 20);
        assertEquals(queryStats.getProcessedInputPositions(), 25);
    }

    private static void assertFinalState(QueryStateMachine stateMachine, QueryState expectedState)
    {
        assertFinalState(stateMachine, expectedState, null);
    }

    private static void assertFinalState(QueryStateMachine stateMachine, QueryState expectedState, Exception expectedException)
    {
        assertTrue(expectedState.isDone());
        assertState(stateMachine, expectedState, expectedException);

        assertFalse(stateMachine.transitionToDispatching());
        assertState(stateMachine, expectedState, expectedException);

        assertFalse(stateMachine.transitionToPlanning());
        assertState(stateMachine, expectedState, expectedException);

        assertFalse(stateMachine.transitionToStarting());
        assertState(stateMachine, expectedState, expectedException);

        assertFalse(stateMachine.transitionToRunning());
        assertState(stateMachine, expectedState, expectedException);

        assertFalse(stateMachine.transitionToFinishing());
        assertState(stateMachine, expectedState, expectedException);

        assertFalse(stateMachine.transitionToFailed(FAILED_CAUSE));
        assertState(stateMachine, expectedState, expectedException);

        // attempt to fail with another exception, which will fail
        assertFalse(stateMachine.transitionToFailed(new IOException("failure after finish")));
        assertState(stateMachine, expectedState, expectedException);
    }

    private static void assertState(QueryStateMachine stateMachine, QueryState expectedState)
    {
        assertState(stateMachine, expectedState, null);
    }

    private static void assertState(QueryStateMachine stateMachine, QueryState expectedState, Exception expectedException)
    {
        assertEquals(stateMachine.getQueryId(), TEST_SESSION.getQueryId());
        assertEqualSessionsWithoutTransactionId(stateMachine.getSession(), TEST_SESSION);
        assertSame(stateMachine.getMemoryPool(), MEMORY_POOL);
        assertEquals(stateMachine.getSetSessionProperties(), SET_SESSION_PROPERTIES);
        assertEquals(stateMachine.getResetSessionProperties(), RESET_SESSION_PROPERTIES);

        QueryInfo queryInfo = stateMachine.getQueryInfo(Optional.empty());
        assertEquals(queryInfo.getQueryId(), TEST_SESSION.getQueryId());
        assertEquals(queryInfo.getSelf(), LOCATION);
        assertFalse(queryInfo.getOutputStage().isPresent());
        assertEquals(queryInfo.getQuery(), QUERY);
        assertEquals(queryInfo.getInputs(), INPUTS);
        assertEquals(queryInfo.getOutput(), OUTPUT);
        assertEquals(queryInfo.getFieldNames(), OUTPUT_FIELD_NAMES);
        assertEquals(queryInfo.getUpdateType(), UPDATE_TYPE);
        assertEquals(queryInfo.getMemoryPool(), MEMORY_POOL.getId());
        assertTrue(queryInfo.getQueryType().isPresent());
        assertEquals(queryInfo.getQueryType().get(), QUERY_TYPE.get());

        QueryStats queryStats = queryInfo.getQueryStats();
        assertNotNull(queryStats.getElapsedTime());
        assertNotNull(queryStats.getQueuedTime());
        assertNotNull(queryStats.getResourceWaitingTime());
        assertNotNull(queryStats.getDispatchingTime());
        assertNotNull(queryStats.getExecutionTime());
        assertNotNull(queryStats.getPlanningTime());
        assertNotNull(queryStats.getFinishingTime());

        assertNotNull(queryStats.getCreateTime());
        if (queryInfo.getState() == QUEUED || queryInfo.getState() == WAITING_FOR_RESOURCES || queryInfo.getState() == DISPATCHING) {
            assertNull(queryStats.getExecutionStartTime());
        }
        else {
            assertNotNull(queryStats.getExecutionStartTime());
        }
        if (queryInfo.getState().isDone()) {
            assertNotNull(queryStats.getEndTime());
        }
        else {
            assertNull(queryStats.getEndTime());
        }

        assertEquals(stateMachine.getQueryState(), expectedState);
        assertEquals(queryInfo.getState(), expectedState);
        assertEquals(stateMachine.isDone(), expectedState.isDone());

        if (expectedState == FAILED) {
            assertNotNull(queryInfo.getFailureInfo());
            FailureInfo failure = queryInfo.getFailureInfo().toFailureInfo();
            assertNotNull(failure);
            assertEquals(failure.getType(), expectedException.getClass().getName());
            if (expectedException instanceof TrinoException) {
                assertEquals(queryInfo.getErrorCode(), ((TrinoException) expectedException).getErrorCode());
            }
            else {
                assertEquals(queryInfo.getErrorCode(), GENERIC_INTERNAL_ERROR.toErrorCode());
            }
        }
        else {
            assertNull(queryInfo.getFailureInfo());
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
                transactionManager,
                emptyEventListenerManager(),
                new AccessControlConfig(),
                DefaultSystemAccessControl.NAME);
        accessControl.setSystemAccessControls(List.of(AllowAllSystemAccessControl.INSTANCE));
        QueryStateMachine stateMachine = QueryStateMachine.beginWithTicker(
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
                QUERY_TYPE);
        stateMachine.setInputs(INPUTS);
        stateMachine.setOutput(OUTPUT);
        stateMachine.setColumns(OUTPUT_FIELD_NAMES, OUTPUT_FIELD_TYPES);
        stateMachine.setUpdateType(UPDATE_TYPE);
        stateMachine.setMemoryPool(MEMORY_POOL);
        for (Entry<String, String> entry : SET_SESSION_PROPERTIES.entrySet()) {
            stateMachine.addSetSessionProperties(entry.getKey(), entry.getValue());
        }
        RESET_SESSION_PROPERTIES.forEach(stateMachine::addResetSessionProperties);
        return stateMachine;
    }

    private static void assertEqualSessionsWithoutTransactionId(Session actual, Session expected)
    {
        assertEquals(actual.getQueryId(), expected.getQueryId());
        assertEquals(actual.getIdentity(), expected.getIdentity());
        assertEquals(actual.getSource(), expected.getSource());
        assertEquals(actual.getCatalog(), expected.getCatalog());
        assertEquals(actual.getSchema(), expected.getSchema());
        assertEquals(actual.getTimeZoneKey(), expected.getTimeZoneKey());
        assertEquals(actual.getLocale(), expected.getLocale());
        assertEquals(actual.getRemoteUserAddress(), expected.getRemoteUserAddress());
        assertEquals(actual.getUserAgent(), expected.getUserAgent());
        assertEquals(actual.getStart(), expected.getStart());
        assertEquals(actual.getSystemProperties(), expected.getSystemProperties());
        assertEquals(actual.getConnectorProperties(), expected.getConnectorProperties());
    }
}
