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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.execution.StateMachine;
import io.trino.execution.TaskTestUtils;
import io.trino.execution.buffer.BufferResult;
import io.trino.execution.buffer.BufferState;
import io.trino.execution.buffer.OutputBuffer;
import io.trino.execution.buffer.OutputBufferInfo;
import io.trino.execution.buffer.OutputBufferStatus;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.execution.buffer.PipelinedOutputBuffers;
import io.trino.metadata.TableHandle;
import io.trino.operator.AlternativesAwareDriverFactory;
import io.trino.operator.SplitDriverFactory;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.sql.planner.LocalExecutionPlanner.LocalExecutionPlan;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ChooseAlternativeNode;
import io.trino.sql.planner.plan.ChooseAlternativeNode.FilteredTableScan;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.TestingMetadata.TestingColumnHandle;
import io.trino.testing.TestingMetadata.TestingTableHandle;
import io.trino.testing.TestingTransactionHandle;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.plan.AggregationNode.globalAggregation;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestLocalExecutionPlannerAlternatives
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;

    PlanBuilder planBuilder = new PlanBuilder(new PlanNodeIdAllocator(), PLANNER_CONTEXT, TEST_SESSION);
    LocalExecutionPlanner planner = TaskTestUtils.createTestingPlanner();

    @BeforeAll
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));
    }

    @AfterAll
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void planChooseAlternative()
    {
        PlanNodeId chooseAlternativeNodeId = new PlanNodeId("chooseAlternativeNodeId");
        Symbol symbol1 = new Symbol(BIGINT, "symbol1");
        Symbol symbol2 = new Symbol(BIGINT, "symbol2");
        ImmutableMap<Symbol, ColumnHandle> symbolMapping = ImmutableMap.of(
                symbol1, new TestingColumnHandle("col1"),
                symbol2, new TestingColumnHandle("col2"));

        PlanNode plan = AggregationNode.singleAggregation(new PlanNodeId("aggregation"),
                new ChooseAlternativeNode(chooseAlternativeNodeId,
                        ImmutableList.of(
                                planBuilder.tableScan(
                                        getTableHandle("alternative1"),
                                        ImmutableList.of(symbol1, symbol2), symbolMapping),
                                planBuilder.tableScan(
                                        getTableHandle("alternative2"),
                                        ImmutableList.of(symbol1, symbol2), symbolMapping)),
                        new FilteredTableScan(planBuilder.tableScan(ImmutableList.of(), false), Optional.empty())),
                ImmutableMap.of(),
                globalAggregation());

        LocalExecutionPlan executionPlan = planner.plan(
                createTaskContext(executor, scheduledExecutor, TEST_SESSION),
                plan,
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), plan.getOutputSymbols()),
                ImmutableList.of(),
                new TestOutputBuffer());

        assertThat(executionPlan.getDriverFactories()).hasSize(1);
        SplitDriverFactory driverFactory = executionPlan.getDriverFactories().get(0);
        assertThat(driverFactory).isInstanceOf(AlternativesAwareDriverFactory.class);
    }

    @Test
    public void chooseAlternativePipelineGetsDifferentPipelineId()
    {
        PlanNodeId chooseAlternativeNodeId = new PlanNodeId("chooseAlternativeNodeId");
        Symbol symbol1 = new Symbol(BIGINT, "symbol1");
        Symbol symbol2 = new Symbol(BIGINT, "symbol2");
        ImmutableMap<Symbol, ColumnHandle> symbolMapping = ImmutableMap.of(
                symbol1, new TestingColumnHandle("col1"),
                symbol2, new TestingColumnHandle("col2"));

        PlanNode plan = AggregationNode.singleAggregation(new PlanNodeId("aggregation"),
                ExchangeNode.partitionedExchange(
                        new PlanNodeId("local exchange"),
                        LOCAL,
                        new ChooseAlternativeNode(chooseAlternativeNodeId,
                                ImmutableList.of(
                                        planBuilder.tableScan(
                                                getTableHandle("alternative1"),
                                                ImmutableList.of(symbol1, symbol2), symbolMapping),
                                        planBuilder.tableScan(
                                                getTableHandle("alternative2"),
                                                ImmutableList.of(symbol1, symbol2), symbolMapping)),
                                new FilteredTableScan(planBuilder.tableScan(ImmutableList.of(), false), Optional.empty())),
                        ImmutableList.of(symbol1),
                        Optional.empty()),
                ImmutableMap.of(),
                globalAggregation());

        LocalExecutionPlan executionPlan = planner.plan(
                createTaskContext(executor, scheduledExecutor, TEST_SESSION),
                plan,
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), plan.getOutputSymbols()),
                ImmutableList.of(),
                new TestOutputBuffer());

        assertThat(executionPlan.getDriverFactories()).hasSize(2);
        SplitDriverFactory alternativesAwareDriverFactory = executionPlan.getDriverFactories().get(0);
        assertThat(alternativesAwareDriverFactory).isInstanceOf(AlternativesAwareDriverFactory.class);
        assertThat(alternativesAwareDriverFactory.getPipelineId()).isEqualTo(0);
        assertThat(executionPlan.getDriverFactories().get(1).getPipelineId()).isEqualTo(1);
    }

    private static TableHandle getTableHandle(String tableName)
    {
        return new TableHandle(TEST_CATALOG_HANDLE, new TestingTableHandle(new SchemaTableName("test", tableName)), TestingTransactionHandle.create());
    }

    private static class TestOutputBuffer
            implements OutputBuffer
    {
        @Override
        public OutputBufferInfo getInfo()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public BufferState getState()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public double getUtilization()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public OutputBufferStatus getStatus()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addStateChangeListener(StateMachine.StateChangeListener<BufferState> stateChangeListener)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setOutputBuffers(OutputBuffers newOutputBuffers)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ListenableFuture<BufferResult> get(PipelinedOutputBuffers.OutputBufferId bufferId, long token, DataSize maxSize)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void acknowledge(PipelinedOutputBuffers.OutputBufferId bufferId, long token)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void destroy(PipelinedOutputBuffers.OutputBufferId bufferId)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ListenableFuture<Void> isFull()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void enqueue(List<Slice> pages)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void enqueue(int partition, List<Slice> pages)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setNoMorePages()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void destroy()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void abort()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getPeakMemoryUsage()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<Throwable> getFailureCause()
        {
            throw new UnsupportedOperationException();
        }
    }
}
