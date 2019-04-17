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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import io.prestosql.Session;
import io.prestosql.connector.CatalogName;
import io.prestosql.memory.context.MemoryTrackingContext;
import io.prestosql.metadata.Split;
import io.prestosql.operator.WorkProcessor.Transformation;
import io.prestosql.operator.WorkProcessor.TransformationState;
import io.prestosql.operator.WorkProcessorAssertion.Transform;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.UpdatablePageSource;
import io.prestosql.sql.planner.plan.PlanNodeId;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.RowPagesBuilder.rowPagesBuilder;
import static io.prestosql.execution.Lifespan.taskWide;
import static io.prestosql.operator.WorkProcessorAssertion.transformationFrom;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.testing.TestingSplit.createLocalSplit;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

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

    @Test
    public void testWorkProcessorPipelineSourceOperator()
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

        Transformation<Page, Page> firstOperatorPages = transformationFrom(ImmutableList.of(
                Transform.of(Optional.of(page1), TransformationState.ofResult(page3, true)),
                Transform.of(Optional.of(page2), TransformationState.ofResult(page4, false)),
                Transform.of(Optional.of(page2), TransformationState.finished())));

        SettableFuture<?> blockedFuture = SettableFuture.create();
        Transformation<Page, Page> secondOperatorPages = transformationFrom(ImmutableList.of(
                Transform.of(Optional.of(page3), TransformationState.ofResult(page5, true)),
                Transform.of(Optional.of(page4), TransformationState.needsMoreData()),
                Transform.of(Optional.empty(), TransformationState.blocked(blockedFuture))));

        TestWorkProcessorSourceOperatorFactory sourceOperatorFactory = new TestWorkProcessorSourceOperatorFactory(
                1,
                new PlanNodeId("1"),
                sourceOperatorPages);
        TestWorkProcessorOperatorFactory firstOperatorFactory = new TestWorkProcessorOperatorFactory(2, firstOperatorPages);
        TestWorkProcessorOperatorFactory secondOperatorFactory = new TestWorkProcessorOperatorFactory(3, secondOperatorPages);

        SourceOperatorFactory pipelineOperatorFactory = (SourceOperatorFactory) getOnlyElement(WorkProcessorPipelineSourceOperator.convertOperators(
                99,
                ImmutableList.of(sourceOperatorFactory, firstOperatorFactory, secondOperatorFactory)));

        DriverContext driverContext = TestingOperatorContext.create(scheduledExecutor).getDriverContext();
        SourceOperator pipelineOperator = pipelineOperatorFactory.createOperator(driverContext);

        // make sure WorkProcessorOperator memory is accounted for
        sourceOperatorFactory.sourceOperator.memoryTrackingContext.localUserMemoryContext().setBytes(123);
        assertEquals(driverContext.getMemoryUsage(), 123);

        assertNull(pipelineOperator.getOutput());
        assertFalse(pipelineOperator.isBlocked().isDone());

        pipelineOperator.addSplit(split);
        assertTrue(pipelineOperator.isBlocked().isDone());

        assertEquals(pipelineOperator.getOutput(), page5);

        // sourceOperator should yield
        driverContext.getYieldSignal().forceYieldForTesting();
        assertNull(pipelineOperator.getOutput());
        driverContext.getYieldSignal().resetYieldForTesting();

        // firstOperatorPages should finish. This should cause sourceOperator and firstOperatorPages to close.
        // secondOperatorPages should block
        assertNull(pipelineOperator.getOutput());
        assertFalse(pipelineOperator.isBlocked().isDone());
        assertTrue(sourceOperatorFactory.sourceOperator.closed);
        assertTrue(firstOperatorFactory.operator.closed);
        assertFalse(secondOperatorFactory.operator.closed);

        // cause early operator finish
        pipelineOperator.finish();

        // operator is still blocked on blockedFuture
        assertFalse(pipelineOperator.isFinished());
        assertTrue(secondOperatorFactory.operator.closed);

        blockedFuture.set(null);
        assertTrue(pipelineOperator.isBlocked().isDone());
        assertNull(pipelineOperator.getOutput());
        assertTrue(pipelineOperator.isFinished());
    }

    private Split createSplit()
    {
        return new Split(
                new CatalogName("catalog_name"),
                createLocalSplit(),
                taskWide());
    }

    private Page createPage(int pageNumber)
    {
        return getOnlyElement(rowPagesBuilder(BIGINT).addSequencePage(1, pageNumber).build());
    }

    private class TestWorkProcessorSourceOperatorFactory
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
        public WorkProcessorSourceOperator create(Session session, MemoryTrackingContext memoryTrackingContext, DriverYieldSignal yieldSignal, WorkProcessor<Split> splits)
        {
            assertNull(sourceOperator, "source operator already created");
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

    private class TestWorkProcessorSourceOperator
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
        public Supplier<Optional<UpdatablePageSource>> getUpdatablePageSourceSupplier()
        {
            return Optional::empty;
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

    private class TestWorkProcessorOperatorFactory
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
        public WorkProcessorOperator create(Session session, MemoryTrackingContext memoryTrackingContext, DriverYieldSignal yieldSignal, WorkProcessor<Page> sourcePages)
        {
            assertNull(operator, "source operator already created");
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

    private class TestWorkProcessorOperator
            implements WorkProcessorOperator
    {
        final WorkProcessor<Page> pages;

        boolean closed;

        TestWorkProcessorOperator(WorkProcessor<Page> pages)
        {
            this.pages = pages;
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
}
