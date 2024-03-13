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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;
import io.trino.cost.StatsAndCosts;
import io.trino.execution.scheduler.SplitSchedulerStats;
import io.trino.operator.DriverContext;
import io.trino.operator.OperatorStats;
import io.trino.operator.PipelineContext;
import io.trino.operator.TaskStats;
import io.trino.operator.TestingOperatorContext;
import io.trino.spi.QueryId;
import io.trino.sql.ir.Row;
import io.trino.sql.ir.StringLiteral;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ValuesNode;
import org.joda.time.DateTime;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.tracing.Tracing.noopTracer;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestStageStateMachine
{
    private static final StageId STAGE_ID = new StageId("query", 0);
    private static final PlanFragment PLAN_FRAGMENT = createValuesPlan();
    private static final SQLException FAILED_CAUSE;

    static {
        FAILED_CAUSE = new SQLException("FAILED");
        FAILED_CAUSE.setStackTrace(new StackTraceElement[0]);
    }

    private ScheduledExecutorService executor = Executors.newScheduledThreadPool(0, daemonThreadsNamed(getClass().getSimpleName() + "-%s"));

    @AfterAll
    public void tearDown()
    {
        executor.shutdownNow();
        executor = null;
    }

    @Test
    public void testBasicStateChanges()
    {
        StageStateMachine stateMachine = createStageStateMachine();
        assertState(stateMachine, StageState.PLANNED);

        assertThat(stateMachine.transitionToScheduling()).isTrue();
        assertState(stateMachine, StageState.SCHEDULING);

        assertThat(stateMachine.transitionToRunning()).isTrue();
        assertState(stateMachine, StageState.RUNNING);

        assertThat(stateMachine.transitionToPending()).isTrue();
        assertState(stateMachine, StageState.PENDING);

        assertThat(stateMachine.transitionToRunning()).isTrue();
        assertState(stateMachine, StageState.RUNNING);

        assertThat(stateMachine.transitionToFinished()).isTrue();
        assertState(stateMachine, StageState.FINISHED);
    }

    @Test
    public void testPlanned()
    {
        StageStateMachine stateMachine = createStageStateMachine();
        assertState(stateMachine, StageState.PLANNED);

        stateMachine = createStageStateMachine();
        assertThat(stateMachine.transitionToScheduling()).isTrue();
        assertState(stateMachine, StageState.SCHEDULING);

        stateMachine = createStageStateMachine();
        assertThat(stateMachine.transitionToRunning()).isTrue();
        assertState(stateMachine, StageState.RUNNING);

        stateMachine = createStageStateMachine();
        assertThat(stateMachine.transitionToFinished()).isTrue();
        assertState(stateMachine, StageState.FINISHED);

        stateMachine = createStageStateMachine();
        assertThat(stateMachine.transitionToFailed(FAILED_CAUSE)).isTrue();
        assertState(stateMachine, StageState.FAILED);
    }

    @Test
    public void testScheduling()
    {
        StageStateMachine stateMachine = createStageStateMachine();
        assertThat(stateMachine.transitionToScheduling()).isTrue();
        assertState(stateMachine, StageState.SCHEDULING);

        assertThat(stateMachine.transitionToScheduling()).isFalse();
        assertState(stateMachine, StageState.SCHEDULING);

        stateMachine = createStageStateMachine();
        stateMachine.transitionToScheduling();
        assertThat(stateMachine.transitionToRunning()).isTrue();
        assertState(stateMachine, StageState.RUNNING);

        stateMachine = createStageStateMachine();
        stateMachine.transitionToScheduling();
        assertThat(stateMachine.transitionToFinished()).isTrue();
        assertState(stateMachine, StageState.FINISHED);

        stateMachine = createStageStateMachine();
        stateMachine.transitionToScheduling();
        assertThat(stateMachine.transitionToFailed(FAILED_CAUSE)).isTrue();
        assertState(stateMachine, StageState.FAILED);
    }

    @Test
    public void testRunning()
    {
        StageStateMachine stateMachine = createStageStateMachine();
        assertThat(stateMachine.transitionToRunning()).isTrue();
        assertState(stateMachine, StageState.RUNNING);

        assertThat(stateMachine.transitionToScheduling()).isFalse();
        assertState(stateMachine, StageState.RUNNING);

        assertThat(stateMachine.transitionToRunning()).isFalse();
        assertState(stateMachine, StageState.RUNNING);

        assertThat(stateMachine.transitionToPending()).isTrue();
        assertState(stateMachine, StageState.PENDING);

        assertThat(stateMachine.transitionToRunning()).isTrue();
        assertState(stateMachine, StageState.RUNNING);

        stateMachine = createStageStateMachine();
        stateMachine.transitionToRunning();
        assertThat(stateMachine.transitionToFinished()).isTrue();
        assertState(stateMachine, StageState.FINISHED);

        stateMachine = createStageStateMachine();
        stateMachine.transitionToRunning();
        assertThat(stateMachine.transitionToFailed(FAILED_CAUSE)).isTrue();
        assertState(stateMachine, StageState.FAILED);
    }

    @Test
    public void testFinished()
    {
        StageStateMachine stateMachine = createStageStateMachine();

        assertThat(stateMachine.transitionToFinished()).isTrue();
        assertFinalState(stateMachine, StageState.FINISHED);
    }

    @Test
    public void testFailed()
    {
        StageStateMachine stateMachine = createStageStateMachine();

        assertThat(stateMachine.transitionToFailed(FAILED_CAUSE)).isTrue();
        assertFinalState(stateMachine, StageState.FAILED);
    }

    @Test
    public void testAlternativeOperatorsNotMerged()
    {
        StageStateMachine stateMachine = createStageStateMachine();
        PipelineContext pipeline0Context = TestingOperatorContext.createDriverContext(executor).getPipelineContext();
        DriverContext alternative0DriverContext = pipeline0Context.addDriverContext();
        alternative0DriverContext.setAlternativePlanContext((transaction, session, columns, dynamicFilter, splitAddressEnforced) -> null, 0);
        alternative0DriverContext.addOperatorContext(0, new PlanNodeId("0"), "operator");
        DriverContext alternative1DriverContext = pipeline0Context.addDriverContext();
        alternative1DriverContext.setAlternativePlanContext((transaction, session, columns, dynamicFilter, splitAddressEnforced) -> null, 1);
        alternative1DriverContext.addOperatorContext(0, new PlanNodeId("0"), "operator");
        pipeline0Context.driverFinished(alternative0DriverContext);
        pipeline0Context.driverFinished(alternative1DriverContext);

        PipelineContext pipeline1Context = TestingOperatorContext.createDriverContext(executor).getPipelineContext();
        DriverContext alternative10DriverContext = pipeline1Context.addDriverContext();
        alternative10DriverContext.setAlternativePlanContext((transaction, session, columns, dynamicFilter, splitAddressEnforced) -> null, 0);
        alternative10DriverContext.addOperatorContext(0, new PlanNodeId("0"), "operator");
        DriverContext alternative11DriverContext = pipeline1Context.addDriverContext();
        alternative11DriverContext.setAlternativePlanContext((transaction, session, columns, dynamicFilter, splitAddressEnforced) -> null, 1);
        alternative11DriverContext.addOperatorContext(0, new PlanNodeId("0"), "operator");
        pipeline1Context.driverFinished(alternative10DriverContext);
        pipeline1Context.driverFinished(alternative11DriverContext);

        StageId stageId = new StageId(new QueryId("0"), 0);
        List<TaskInfo> taskInfoList = ImmutableList.of(
                TaskInfo.createInitialTask(
                        new TaskId(stageId, 0, 0),
                        URI.create(""),
                        "0",
                        false,
                        Optional.empty(),
                        taskStats(ImmutableList.of(pipeline0Context, pipeline1Context))));
        StageInfo stageInfo = stateMachine.getStageInfo(() -> taskInfoList);

        List<OperatorStats> operatorSummaries = stageInfo.getStageStats().getOperatorSummaries();
        assertThat(operatorSummaries.size()).isEqualTo(2);
        assertThat(operatorSummaries.get(0).getOperatorId()).isEqualTo(0);
        assertThat(operatorSummaries.get(1).getOperatorId()).isEqualTo(0);
        assertThat(operatorSummaries.get(0).getAlternativeId()).isNotEqualTo(operatorSummaries.get(1).getAlternativeId());
    }

    @Test
    public void testOperatorsMerged()
    {
        StageStateMachine stateMachine = createStageStateMachine();
        PipelineContext pipeline0Context = TestingOperatorContext.createDriverContext(executor).getPipelineContext();
        DriverContext alternative0DriverContext = pipeline0Context.addDriverContext();
        alternative0DriverContext.setAlternativePlanContext((transaction, session, columns, dynamicFilter, splitAddressEnforced) -> null, 2);
        alternative0DriverContext.addOperatorContext(0, new PlanNodeId("0"), "operator");
        DriverContext alternative1DriverContext = pipeline0Context.addDriverContext();
        alternative1DriverContext.setAlternativePlanContext((transaction, session, columns, dynamicFilter, splitAddressEnforced) -> null, 2);
        alternative1DriverContext.addOperatorContext(0, new PlanNodeId("0"), "operator");
        pipeline0Context.driverFinished(alternative0DriverContext);
        pipeline0Context.driverFinished(alternative1DriverContext);

        PipelineContext pipeline1Context = TestingOperatorContext.createDriverContext(executor).getPipelineContext();
        DriverContext alternative10DriverContext = pipeline1Context.addDriverContext();
        alternative10DriverContext.setAlternativePlanContext((transaction, session, columns, dynamicFilter, splitAddressEnforced) -> null, 2);
        alternative10DriverContext.addOperatorContext(0, new PlanNodeId("0"), "operator");
        DriverContext alternative11DriverContext = pipeline1Context.addDriverContext();
        alternative11DriverContext.setAlternativePlanContext((transaction, session, columns, dynamicFilter, splitAddressEnforced) -> null, 2);
        alternative11DriverContext.addOperatorContext(0, new PlanNodeId("0"), "operator");
        pipeline1Context.driverFinished(alternative10DriverContext);
        pipeline1Context.driverFinished(alternative11DriverContext);

        StageId stageId = new StageId(new QueryId("0"), 0);
        List<TaskInfo> taskInfoList = ImmutableList.of(
                TaskInfo.createInitialTask(
                        new TaskId(stageId, 0, 0),
                        URI.create(""),
                        "0",
                        false,
                        Optional.empty(),
                        taskStats(ImmutableList.of(pipeline0Context, pipeline1Context))));
        StageInfo stageInfo = stateMachine.getStageInfo(() -> taskInfoList);

        List<OperatorStats> operatorSummaries = stageInfo.getStageStats().getOperatorSummaries();
        assertThat(operatorSummaries.size()).isEqualTo(1);
        assertThat(operatorSummaries.get(0).getOperatorId()).isEqualTo(0);
        assertThat(operatorSummaries.get(0).getAlternativeId()).isEqualTo(2);
    }

    private static TaskStats taskStats(List<PipelineContext> pipelineContexts)
    {
        return new TaskStats(DateTime.now(),
                null,
                null,
                null,
                null,
                null,
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                0,
                0,
                0,
                0L,
                0,
                0,
                0L,
                0,
                0,
                0.0,
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                Duration.ZERO,
                new Duration(0, MILLISECONDS),
                Duration.ZERO,
                false,
                ImmutableSet.of(),
                DataSize.ofBytes(0),
                0,
                new Duration(0, MILLISECONDS),
                DataSize.ofBytes(0),
                0,
                DataSize.ofBytes(0),
                0,
                DataSize.ofBytes(0),
                0,
                new Duration(0, MILLISECONDS),
                DataSize.ofBytes(0),
                0,
                new Duration(0, MILLISECONDS),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                Optional.empty(),
                0,
                new Duration(0, MILLISECONDS),
                pipelineContexts.stream().map(PipelineContext::getPipelineStats).collect(toImmutableList()));
    }

    private static void assertFinalState(StageStateMachine stateMachine, StageState expectedState)
    {
        assertThat(expectedState.isDone()).isTrue();

        assertState(stateMachine, expectedState);

        assertThat(stateMachine.transitionToScheduling()).isFalse();
        assertState(stateMachine, expectedState);

        assertThat(stateMachine.transitionToPending()).isFalse();
        assertState(stateMachine, expectedState);

        assertThat(stateMachine.transitionToRunning()).isFalse();
        assertState(stateMachine, expectedState);

        assertThat(stateMachine.transitionToFinished()).isFalse();
        assertState(stateMachine, expectedState);

        assertThat(stateMachine.transitionToFailed(FAILED_CAUSE)).isFalse();
        assertState(stateMachine, expectedState);

        // attempt to fail with another exception, which will fail
        assertThat(stateMachine.transitionToFailed(new IOException("failure after finish"))).isFalse();
        assertState(stateMachine, expectedState);
    }

    private static void assertState(StageStateMachine stateMachine, StageState expectedState)
    {
        assertThat(stateMachine.getStageId()).isEqualTo(STAGE_ID);

        StageInfo stageInfo = stateMachine.getStageInfo(ImmutableList::of);
        assertThat(stageInfo.getStageId()).isEqualTo(STAGE_ID);
        assertThat(stageInfo.getSubStages()).isEqualTo(ImmutableList.of());
        assertThat(stageInfo.getTasks()).isEqualTo(ImmutableList.of());
        assertThat(stageInfo.getTypes()).isEqualTo(ImmutableList.of(VARCHAR));
        assertThat(stageInfo.getPlan()).isSameAs(PLAN_FRAGMENT);

        assertThat(stateMachine.getState()).isEqualTo(expectedState);
        assertThat(stageInfo.getState()).isEqualTo(expectedState);

        if (expectedState == StageState.FAILED) {
            ExecutionFailureInfo failure = stageInfo.getFailureCause();
            assertThat(failure.getMessage()).isEqualTo(FAILED_CAUSE.getMessage());
            assertThat(failure.getType()).isEqualTo(FAILED_CAUSE.getClass().getName());
        }
        else {
            assertThat(stageInfo.getFailureCause()).isNull();
        }
    }

    private StageStateMachine createStageStateMachine()
    {
        return new StageStateMachine(
                STAGE_ID,
                PLAN_FRAGMENT,
                ImmutableMap.of(),
                executor,
                noopTracer(),
                Span.getInvalid(),
                new SplitSchedulerStats());
    }

    private static PlanFragment createValuesPlan()
    {
        Symbol symbol = new Symbol("column");
        PlanNodeId valuesNodeId = new PlanNodeId("plan");
        PlanFragment planFragment = new PlanFragment(
                new PlanFragmentId("plan"),
                new ValuesNode(valuesNodeId,
                        ImmutableList.of(symbol),
                        ImmutableList.of(new Row(ImmutableList.of(new StringLiteral("foo"))))),
                ImmutableMap.of(symbol, VARCHAR),
                SOURCE_DISTRIBUTION,
                Optional.empty(),
                ImmutableList.of(valuesNodeId),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(symbol)),
                StatsAndCosts.empty(),
                ImmutableList.of(),
                ImmutableList.of(),
                Optional.empty());

        return planFragment;
    }
}
