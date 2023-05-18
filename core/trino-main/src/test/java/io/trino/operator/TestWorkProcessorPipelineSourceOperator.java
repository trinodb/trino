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
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.memory.context.MemoryTrackingContext;
import io.trino.metadata.Split;
import io.trino.operator.WorkProcessor.Transformation;
import io.trino.operator.WorkProcessor.TransformationState;
import io.trino.operator.WorkProcessorAssertion.Transform;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.Page;
import io.trino.spi.metrics.Metrics;
import io.trino.sql.planner.LocalExecutionPlanner.OperatorFactoryWithTypes;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.TestingTaskContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.WorkProcessorAssertion.transformationFrom;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingSplit.createLocalSplit;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestWorkProcessorPipelineSourceOperator
{
    private ScheduledExecutorService scheduledExecutor;

    @BeforeClass
    public void setUp()
    {
        scheduledExecutor = newSingleThreadScheduledExecutor();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        scheduledExecutor.shutdownNow();
    }

    @Test(timeOut = 10_000)
    public void testWorkProcessorPipelineSourceOperator()
            throws InterruptedException
    {
        Split split = createSplit();

        Page page1 = createPage(1);
        Page page2 = createPage(2);
        Page page3 = createPage(3);
        Page page4 = createPage(4);
        Page page5 = createPage(5);

        Transformation<Split, Page> sourceOperatorPages = transformationFrom(ImmutableList.of(
                Transform.of(Optional.of(split), TransformationState.ofResult(page1, false)),
                Transform.of(Optional.of(split), TransformationState.ofResult(page2, true))));

        SettableFuture<Void> firstBlockedFuture = SettableFuture.create();
        Transformation<Page, Page> firstOperatorPages = transformationFrom(ImmutableList.of(
                        Transform.of(Optional.of(page1), TransformationState.blocked(firstBlockedFuture)),
                        Transform.of(Optional.of(page1), TransformationState.ofResult(page3, true)),
                        Transform.of(Optional.of(page2), TransformationState.ofResult(page4, false)),
                        Transform.of(Optional.of(page2), TransformationState.finished())),
                (left, right) -> left.getPositionCount() == right.getPositionCount());

        SettableFuture<Void> secondBlockedFuture = SettableFuture.create();
        Transformation<Page, Page> secondOperatorPages = transformationFrom(ImmutableList.of(
                        Transform.of(Optional.of(page3), TransformationState.ofResult(page5, true)),
                        Transform.of(Optional.of(page4), TransformationState.needsMoreData()),
                        Transform.of(Optional.empty(), TransformationState.blocked(secondBlockedFuture))),
                (left, right) -> left.getPositionCount() == right.getPositionCount());

        TestWorkProcessorSourceOperatorFactory sourceOperatorFactory = new TestWorkProcessorSourceOperatorFactory(
                1,
                new PlanNodeId("1"),
                sourceOperatorPages);
        TestWorkProcessorOperatorFactory firstOperatorFactory = new TestWorkProcessorOperatorFactory(2, firstOperatorPages);
        TestWorkProcessorOperatorFactory secondOperatorFactory = new TestWorkProcessorOperatorFactory(3, secondOperatorPages);

        SourceOperatorFactory pipelineOperatorFactory = (SourceOperatorFactory) getOnlyElement(WorkProcessorPipelineSourceOperator.convertOperators(
                ImmutableList.of(
                        new OperatorFactoryWithTypes(sourceOperatorFactory, ImmutableList.of(BIGINT)),
                        new OperatorFactoryWithTypes(firstOperatorFactory, ImmutableList.of(BIGINT)),
                        new OperatorFactoryWithTypes(secondOperatorFactory, ImmutableList.of(BIGINT))),
                DataSize.ofBytes(0),
                0));

        DriverContext driverContext = TestingTaskContext.builder(MoreExecutors.directExecutor(), scheduledExecutor, TEST_SESSION)
                .build()
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
        SourceOperator pipelineOperator = pipelineOperatorFactory.createOperator(driverContext);
        OperatorContext pipelineOperatorContext = pipelineOperator.getOperatorContext();

        // make sure WorkProcessorOperator memory is accounted for
        sourceOperatorFactory.sourceOperator.memoryTrackingContext.localUserMemoryContext().setBytes(123);
        assertThat(driverContext.getMemoryUsage()).isEqualTo(123);

        assertThat(pipelineOperator.getOutput()).isNull();
        assertThat(pipelineOperator.isBlocked().isDone()).isFalse();
        // blocking on splits should not account for blocked time for any WorkProcessorOperator
        pipelineOperatorContext.getNestedOperatorStats().forEach(
                operatorStats -> assertThat(operatorStats.getBlockedWall().toMillis()).isEqualTo(0));

        pipelineOperator.addSplit(split);
        assertThat(pipelineOperator.isBlocked().isDone()).isTrue();

        assertThat(pipelineOperator.getOutput()).isNull();
        assertThat(pipelineOperator.isBlocked().isDone()).isFalse();

        Thread.sleep(100);
        firstBlockedFuture.set(null);
        assertThat(pipelineOperator.isBlocked().isDone()).isTrue();

        // blocking of first WorkProcessorOperator should be accounted for in stats
        List<OperatorStats> operatorStats = pipelineOperatorContext.getNestedOperatorStats();
        assertThat(operatorStats.get(0).getBlockedWall().toMillis()).isEqualTo(0);
        assertThat(operatorStats.get(1).getBlockedWall().toMillis()).isPositive();
        assertThat(operatorStats.get(2).getBlockedWall().toMillis()).isEqualTo(0);

        assertThat(getTestingOperatorInfo(operatorStats.get(1)).count).isEqualTo(2);
        assertThat(getTestingOperatorInfo(operatorStats.get(2)).count).isEqualTo(2);

        assertThat(pipelineOperator.getOutput().getPositionCount()).isEqualTo(page5.getPositionCount());

        // sourceOperator should yield
        driverContext.getYieldSignal().forceYieldForTesting();
        assertThat(pipelineOperator.getOutput()).isNull();
        driverContext.getYieldSignal().resetYieldForTesting();

        // firstOperatorPages should finish. This should cause sourceOperator and firstOperatorPages to close.
        // secondOperatorPages should block
        assertThat(pipelineOperator.getOutput()).isNull();
        assertThat(pipelineOperator.isBlocked().isDone()).isFalse();
        assertThat(sourceOperatorFactory.sourceOperator.closed).isTrue();
        assertThat(firstOperatorFactory.operator.closed).isTrue();
        assertThat(secondOperatorFactory.operator.closed).isFalse();

        // first operator should return final operator info
        assertThat(getTestingOperatorInfo(operatorStats.get(1)).count).isEqualTo(3);
        assertThat(getTestingOperatorInfo(operatorStats.get(2)).count).isEqualTo(2);
        operatorStats = pipelineOperatorContext.getNestedOperatorStats();
        assertThat(getTestingOperatorInfo(operatorStats.get(1)).count).isEqualTo(3);
        assertThat(getTestingOperatorInfo(operatorStats.get(2)).count).isEqualTo(3);

        // cause early operator finish
        pipelineOperator.finish();

        // operator is still blocked on secondBlockedFuture
        assertThat(pipelineOperator.isFinished()).isFalse();
        assertThat(secondOperatorFactory.operator.closed).isTrue();

        secondBlockedFuture.set(null);
        assertThat(pipelineOperator.isBlocked().isDone()).isTrue();
        assertThat(pipelineOperator.getOutput()).isNull();
        assertThat(pipelineOperator.isFinished()).isTrue();

        // assert operator stats are correct
        operatorStats = pipelineOperatorContext.getNestedOperatorStats();
        assertThat(operatorStats.get(0).getOutputPositions()).isEqualTo(3);
        assertThat(operatorStats.get(1).getInputPositions()).isEqualTo(3);
        assertThat(operatorStats.get(0).getOutputDataSize().toBytes()).isEqualTo(27);
        assertThat(operatorStats.get(1).getInputDataSize().toBytes()).isEqualTo(27);

        assertThat(operatorStats.get(1).getOutputPositions()).isEqualTo(7);
        assertThat(operatorStats.get(2).getInputPositions()).isEqualTo(7);
        assertThat(operatorStats.get(1).getOutputDataSize().toBytes()).isEqualTo(63);
        assertThat(operatorStats.get(2).getInputDataSize().toBytes()).isEqualTo(63);

        assertThat(operatorStats.get(2).getOutputPositions()).isEqualTo(5);
        assertThat(operatorStats.get(2).getOutputDataSize().toBytes()).isEqualTo(45);

        assertThat(operatorStats.get(1).getMetrics().getMetrics())
                .hasSize(5)
                .containsEntry("testOperatorMetric", new LongCount(1));

        // assert source operator stats are correct
        OperatorStats sourceOperatorStats = operatorStats.get(0);

        assertThat(sourceOperatorStats.getMetrics().getMetrics())
                .hasSize(6)
                .containsEntry("testSourceMetric", new LongCount(1))
                .containsEntry("testSourceClosed", new LongCount(1));
        assertThat(sourceOperatorStats.getConnectorMetrics().getMetrics()).isEqualTo(ImmutableMap.of(
                "testSourceConnectorMetric", new LongCount(2),
                "testSourceConnectorClosed", new LongCount(1)));

        assertThat(sourceOperatorStats.getDynamicFilterSplitsProcessed()).isEqualTo(42L);

        assertThat(sourceOperatorStats.getPhysicalInputDataSize()).isEqualTo(DataSize.ofBytes(1));
        assertThat(sourceOperatorStats.getPhysicalInputPositions()).isEqualTo(2);

        assertThat(sourceOperatorStats.getInternalNetworkInputDataSize()).isEqualTo(DataSize.ofBytes(3));
        assertThat(sourceOperatorStats.getInternalNetworkInputPositions()).isEqualTo(4);

        assertThat(sourceOperatorStats.getInputDataSize()).isEqualTo(DataSize.ofBytes(5));
        assertThat(sourceOperatorStats.getInputPositions()).isEqualTo(6);

        assertThat(sourceOperatorStats.getAddInputWall()).isEqualTo(new Duration(0, NANOSECONDS));

        // pipeline input stats should match source WorkProcessorOperator stats
        PipelineStats pipelineStats = pipelineOperator.getOperatorContext().getDriverContext().getPipelineContext().getPipelineStats();
        assertThat(sourceOperatorStats.getPhysicalInputDataSize()).isEqualTo(pipelineStats.getPhysicalInputDataSize());
        assertThat(sourceOperatorStats.getPhysicalInputPositions()).isEqualTo(pipelineStats.getPhysicalInputPositions());

        assertThat(sourceOperatorStats.getInternalNetworkInputDataSize()).isEqualTo(pipelineStats.getInternalNetworkInputDataSize());
        assertThat(sourceOperatorStats.getInternalNetworkInputPositions()).isEqualTo(pipelineStats.getInternalNetworkInputPositions());

        assertThat(sourceOperatorStats.getInputDataSize()).isEqualTo(pipelineStats.getProcessedInputDataSize());
        assertThat(sourceOperatorStats.getInputPositions()).isEqualTo(pipelineStats.getProcessedInputPositions());

        assertThat(sourceOperatorStats.getPhysicalInputReadTime().convertToMostSuccinctTimeUnit())
                .isEqualTo(pipelineStats.getPhysicalInputReadTime().convertToMostSuccinctTimeUnit());

        // assert pipeline metrics
        List<OperatorStats> operatorSummaries = pipelineStats.getOperatorSummaries();
        assertThat(operatorSummaries.get(0).getMetrics().getMetrics())
                .hasSize(6)
                .containsEntry("testSourceMetric", new LongCount(1))
                .containsEntry("testSourceClosed", new LongCount(1));
        assertThat(operatorSummaries.get(0).getConnectorMetrics().getMetrics()).isEqualTo(ImmutableMap.of(
                "testSourceConnectorMetric", new LongCount(2),
                "testSourceConnectorClosed", new LongCount(1)));
        assertThat(operatorSummaries.get(1).getMetrics().getMetrics())
                .hasSize(5)
                .containsEntry("testOperatorMetric", new LongCount(1));
    }

    @Test
    public void testMergePages()
    {
        Transformation<Split, Page> sourceOperatorPages = split -> TransformationState.ofResult(createPage(1), false);
        Transformation<Page, Page> firstOperatorPages = page -> TransformationState.ofResult(
                getOnlyElement(rowPagesBuilder(BIGINT).addSequencePage(1, 0).build()));

        TestWorkProcessorSourceOperatorFactory sourceOperatorFactory = new TestWorkProcessorSourceOperatorFactory(
                1,
                new PlanNodeId("1"),
                sourceOperatorPages);
        TestWorkProcessorOperatorFactory firstOperatorFactory = new TestWorkProcessorOperatorFactory(2, firstOperatorPages);

        SourceOperatorFactory pipelineOperatorFactory = (SourceOperatorFactory) getOnlyElement(WorkProcessorPipelineSourceOperator.convertOperators(
                ImmutableList.of(
                        new OperatorFactoryWithTypes(sourceOperatorFactory, ImmutableList.of(BIGINT)),
                        new OperatorFactoryWithTypes(firstOperatorFactory, ImmutableList.of(BIGINT))),
                DataSize.ofBytes(100),
                100));

        DriverContext driverContext = TestingOperatorContext.create(scheduledExecutor).getDriverContext();
        SourceOperator pipelineOperator = pipelineOperatorFactory.createOperator(driverContext);
        pipelineOperator.addSplit(createSplit());

        assertThat(pipelineOperator.getOutput().getPositionCount()).isGreaterThan(100);
    }

    private TestOperatorInfo getTestingOperatorInfo(OperatorStats operatorStats)
    {
        return (TestOperatorInfo) operatorStats.getInfo();
    }

    private Split createSplit()
    {
        return new Split(
                TEST_CATALOG_HANDLE,
                createLocalSplit());
    }

    private Page createPage(int pageNumber)
    {
        return getOnlyElement(rowPagesBuilder(BIGINT).addSequencePage(pageNumber, pageNumber).build());
    }

    private static class TestWorkProcessorSourceOperatorFactory
            implements WorkProcessorSourceOperatorFactory, SourceOperatorFactory
    {
        final int operatorId;
        final PlanNodeId sourceId;
        final Transformation<Split, Page> transformation;

        TestWorkProcessorSourceOperator sourceOperator;

        TestWorkProcessorSourceOperatorFactory(int operatorId, PlanNodeId sourceId, Transformation<Split, Page> transformation)
        {
            this.operatorId = operatorId;
            this.sourceId = sourceId;
            this.transformation = transformation;
        }

        @Override
        public int getOperatorId()
        {
            return operatorId;
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return sourceId;
        }

        @Override
        public PlanNodeId getPlanNodeId()
        {
            return sourceId;
        }

        @Override
        public String getOperatorType()
        {
            return TestWorkProcessorSourceOperatorFactory.class.getSimpleName();
        }

        @Override
        public WorkProcessorSourceOperator create(Session session, MemoryTrackingContext memoryTrackingContext, DriverYieldSignal yieldSignal, WorkProcessor<Split> splits)
        {
            assertThat(sourceOperator).withFailMessage("source operator already created").isNull();
            sourceOperator = new TestWorkProcessorSourceOperator(
                    splits
                            .transform(transformation)
                            .yielding(yieldSignal::isSet),
                    memoryTrackingContext);
            return sourceOperator;
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void noMoreOperators()
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class TestWorkProcessorSourceOperator
            implements WorkProcessorSourceOperator
    {
        final WorkProcessor<Page> pages;

        boolean closed;
        MemoryTrackingContext memoryTrackingContext;

        TestWorkProcessorSourceOperator(WorkProcessor<Page> pages, MemoryTrackingContext memoryTrackingContext)
        {
            this.pages = pages;
            this.memoryTrackingContext = memoryTrackingContext;
        }

        @Override
        public DataSize getPhysicalInputDataSize()
        {
            return DataSize.ofBytes(1);
        }

        @Override
        public long getPhysicalInputPositions()
        {
            return 2;
        }

        @Override
        public DataSize getInternalNetworkInputDataSize()
        {
            return DataSize.ofBytes(3);
        }

        @Override
        public long getInternalNetworkPositions()
        {
            return 4;
        }

        @Override
        public DataSize getInputDataSize()
        {
            return DataSize.ofBytes(5);
        }

        @Override
        public long getInputPositions()
        {
            return 6;
        }

        @Override
        public Duration getReadTime()
        {
            return new Duration(7, NANOSECONDS);
        }

        @Override
        public long getDynamicFilterSplitsProcessed()
        {
            return 42;
        }

        @Override
        public Metrics getMetrics()
        {
            return new Metrics(ImmutableMap.of(
                    "testSourceMetric", new LongCount(1),
                    "testSourceClosed", new LongCount(closed ? 1 : 0)));
        }

        @Override
        public Metrics getConnectorMetrics()
        {
            return new Metrics(ImmutableMap.of(
                    "testSourceConnectorMetric", new LongCount(2),
                    "testSourceConnectorClosed", new LongCount(closed ? 1 : 0)));
        }

        @Override
        public WorkProcessor<Page> getOutputPages()
        {
            return pages;
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private static class TestWorkProcessorOperatorFactory
            implements WorkProcessorOperatorFactory, OperatorFactory
    {
        final int operatorId;
        final Transformation<Page, Page> transformation;

        TestWorkProcessorOperator operator;

        TestWorkProcessorOperatorFactory(int operatorId, Transformation<Page, Page> transformation)
        {
            this.operatorId = operatorId;
            this.transformation = transformation;
        }

        @Override
        public int getOperatorId()
        {
            return operatorId;
        }

        @Override
        public PlanNodeId getPlanNodeId()
        {
            return new PlanNodeId("test-operator");
        }

        @Override
        public String getOperatorType()
        {
            return TestWorkProcessorOperatorFactory.class.getSimpleName();
        }

        @Override
        public WorkProcessorOperator create(ProcessorContext processorContext, WorkProcessor<Page> sourcePages)
        {
            assertThat(operator).withFailMessage("source operator already created").isNull();
            operator = new TestWorkProcessorOperator(sourcePages.transform(transformation));
            return operator;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void noMoreOperators()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public OperatorFactory duplicate()
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class TestWorkProcessorOperator
            implements WorkProcessorOperator
    {
        final WorkProcessor<Page> pages;
        final TestOperatorInfo operatorInfo = new TestOperatorInfo();

        boolean closed;

        TestWorkProcessorOperator(WorkProcessor<Page> pages)
        {
            this.pages = pages;
        }

        @Override
        public Optional<OperatorInfo> getOperatorInfo()
        {
            operatorInfo.count++;
            return Optional.of(operatorInfo);
        }

        @Override
        public Metrics getMetrics()
        {
            return new Metrics(ImmutableMap.of("testOperatorMetric", new LongCount(1)));
        }

        @Override
        public WorkProcessor<Page> getOutputPages()
        {
            return pages;
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private static class TestOperatorInfo
            implements OperatorInfo
    {
        int count;
    }
}
