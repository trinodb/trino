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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.execution.TableExecuteContext;
import io.trino.execution.TableExecuteContextManager;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.TableFinishOperator.TableFinishOperatorFactory;
import io.trino.operator.TableFinishOperator.TableFinisher;
import io.trino.operator.aggregation.TestingAggregationFunction;
import io.trino.spi.block.LongArrayBlockBuilder;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.statistics.ColumnStatisticMetadata;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.StatisticAggregationsDescriptor;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.operator.PageAssertions.assertPageEquals;
import static io.trino.spi.statistics.ColumnStatisticType.MAX_VALUE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestTableFinishOperator
{
    private static final TestingAggregationFunction LONG_MAX = new TestingFunctionResolution().getAggregateFunction("max", fromTypes(BIGINT));

    private ScheduledExecutorService scheduledExecutor;

    @BeforeAll
    public void setUp()
    {
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));
    }

    @AfterAll
    public void tearDown()
    {
        scheduledExecutor.shutdownNow();
        scheduledExecutor = null;
    }

    @Test
    public void testStatisticsAggregation()
            throws Exception
    {
        TestTableFinisher tableFinisher = new TestTableFinisher();
        ColumnStatisticMetadata statisticMetadata = new ColumnStatisticMetadata("column", MAX_VALUE);
        StatisticAggregationsDescriptor<Integer> descriptor = new StatisticAggregationsDescriptor<>(
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(statisticMetadata, 0));
        Session session = testSessionBuilder()
                .setSystemProperty("statistics_cpu_timer_enabled", "true")
                .build();
        TableExecuteContextManager tableExecuteContextManager = new TableExecuteContextManager();
        TableFinishOperatorFactory operatorFactory = new TableFinishOperatorFactory(
                0,
                new PlanNodeId("node"),
                tableFinisher,
                new AggregationOperator.AggregationOperatorFactory(
                        1,
                        new PlanNodeId("test"),
                        ImmutableList.of(LONG_MAX.createAggregatorFactory(SINGLE, ImmutableList.of(2), OptionalInt.empty()))),
                descriptor,
                tableExecuteContextManager,
                true,
                session);
        DriverContext driverContext = createTaskContext(scheduledExecutor, scheduledExecutor, session)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
        tableExecuteContextManager.registerTableExecuteContextForQuery(driverContext.getPipelineContext().getTaskContext().getQueryContext().getQueryId());
        TableFinishOperator operator = (TableFinishOperator) operatorFactory.createOperator(driverContext);

        List<Type> inputTypes = ImmutableList.of(BIGINT, VARBINARY, BIGINT);

        operator.addInput(rowPagesBuilder(inputTypes).row(4, null, null).build().get(0));
        operator.addInput(rowPagesBuilder(inputTypes).row(5, null, null).build().get(0));
        operator.addInput(rowPagesBuilder(inputTypes).row(null, new byte[] {1}, null).build().get(0));
        operator.addInput(rowPagesBuilder(inputTypes).row(null, new byte[] {2}, null).build().get(0));
        operator.addInput(rowPagesBuilder(inputTypes).row(null, null, 6).build().get(0));
        operator.addInput(rowPagesBuilder(inputTypes).row(null, null, 7).build().get(0));

        assertThat(driverContext.getMemoryUsage()).as("memoryUsage").isGreaterThan(0);

        assertThat(operator.isBlocked().isDone())
                .describedAs("isBlocked should be done")
                .isTrue();
        assertThat(operator.needsInput())
                .describedAs("needsInput should be true")
                .isTrue();

        operator.finish();
        assertThat(operator.isFinished())
                .describedAs("isFinished should be false")
                .isFalse();

        assertThat(operator.getOutput()).isNull();
        List<Type> outputTypes = ImmutableList.of(BIGINT);
        assertPageEquals(outputTypes, operator.getOutput(), rowPagesBuilder(outputTypes).row(9).build().get(0));

        assertThat(operator.isBlocked().isDone())
                .describedAs("isBlocked should be done")
                .isTrue();
        assertThat(operator.needsInput())
                .describedAs("needsInput should be false")
                .isFalse();
        assertThat(operator.isFinished())
                .describedAs("isFinished should be true")
                .isTrue();

        operator.close();

        assertThat(tableFinisher.getFragments()).isEqualTo(ImmutableList.of(Slices.wrappedBuffer(new byte[] {1}), Slices.wrappedBuffer(new byte[] {2})));
        assertThat(tableFinisher.getComputedStatistics()).hasSize(1);
        assertThat(getOnlyElement(tableFinisher.getComputedStatistics()).getColumnStatistics()).hasSize(1);

        LongArrayBlockBuilder expectedStatistics = new LongArrayBlockBuilder(null, 1);
        BIGINT.writeLong(expectedStatistics, 7);
        assertBlockEquals(BIGINT, getOnlyElement(tableFinisher.getComputedStatistics()).getColumnStatistics().get(statisticMetadata), expectedStatistics.build());

        assertThat(driverContext.getMemoryUsage())
                .describedAs("memoryUsage")
                .isEqualTo(0);
    }

    private static class TestTableFinisher
            implements TableFinisher
    {
        private boolean finished;
        private Collection<Slice> fragments;
        private Collection<ComputedStatistics> computedStatistics;
        private TableExecuteContext tableExecuteContext;

        @Override
        public Optional<ConnectorOutputMetadata> finishTable(Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics, TableExecuteContext tableExecuteContext)
        {
            checkState(!finished, "already finished");
            finished = true;
            this.fragments = fragments;
            this.computedStatistics = computedStatistics;
            this.tableExecuteContext = tableExecuteContext;
            return Optional.empty();
        }

        public Collection<Slice> getFragments()
        {
            return fragments;
        }

        public Collection<ComputedStatistics> getComputedStatistics()
        {
            return computedStatistics;
        }

        public TableExecuteContext getTableExecuteContext()
        {
            return tableExecuteContext;
        }
    }
}
