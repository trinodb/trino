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
package io.trino.operator.join.unspilled;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.trino.operator.DriverContext;
import io.trino.operator.OperatorContext;
import io.trino.operator.PagesIndex;
import io.trino.operator.TaskContext;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.TestingTaskContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.SequencePageBuilder.createSequencePage;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.HashArraySizeSupplier.defaultHashArraySizeSupplier;
import static io.trino.operator.join.unspilled.HashBuilderOperator.State.CLOSED;
import static io.trino.operator.join.unspilled.HashBuilderOperator.State.CONSUMING_INPUT;
import static io.trino.operator.join.unspilled.HashBuilderOperator.State.LOOKUP_SOURCE_BUILT;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestHashBuilderOperator
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;

    @BeforeClass
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
        if (scheduledExecutor != null) {
            scheduledExecutor.shutdownNow();
            scheduledExecutor = null;
        }
    }

    @Test
    public void test()
    {
        long memoryPoolSizeInBytes = DataSize.of(1, MEGABYTE).toBytes();
        TaskContext taskContext = TestingTaskContext.builder(executor, scheduledExecutor, TEST_SESSION)
                .setMemoryPoolSize(DataSize.ofBytes(memoryPoolSizeInBytes))
                .build();
        DriverContext driverContext = taskContext
                .addPipelineContext(0, false, false, false)
                .addDriverContext();
        OperatorContext operatorContext = driverContext
                .addOperatorContext(0, new PlanNodeId("0"), HashBuilderOperator.class.getName());
        OperatorContext anotherOperatorContext = driverContext
                .addOperatorContext(1, new PlanNodeId("1"), "another operator");
        ImmutableList<Type> types = ImmutableList.of(BIGINT, BIGINT);
        PartitionedLookupSourceFactory lookupSourceFactory = new PartitionedLookupSourceFactory(
                types,
                ImmutableList.of(BIGINT),
                ImmutableList.of(BIGINT),
                1,
                false,
                new TypeOperators());
        try (HashBuilderOperator operator = new HashBuilderOperator(
                operatorContext,
                lookupSourceFactory,
                0,
                ImmutableList.of(0),
                ImmutableList.of(1),
                OptionalInt.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(),
                10_000,
                new PagesIndex.TestingFactory(false),
                defaultHashArraySizeSupplier())) {
            assertEquals(operator.getState(), CONSUMING_INPUT);

            ListenableFuture<Void> whenBuildFinishes = lookupSourceFactory.whenBuildFinishes();
            assertThat(whenBuildFinishes).isNotDone();

            for (int i = 0; i < 100; i++) {
                assertThat(operator.isBlocked()).isDone();
                assertTrue(operator.needsInput());
                operator.addInput(somePage(types));
            }

            assertFalse(operator.isFinished());
            assertEquals(operator.getState(), CONSUMING_INPUT);

            anotherOperatorContext.getOperatorMemoryContext().localUserMemoryContext().setBytes(memoryPoolSizeInBytes);

            operator.finish();

            // not enough memory to create lookup source
            assertEquals(operator.getState(), CONSUMING_INPUT);
            assertFalse(operator.isFinished());
            assertThat(whenBuildFinishes).isNotDone();
            assertThat(operatorContext.isWaitingForMemory()).isNotDone();

            anotherOperatorContext.getOperatorMemoryContext().localUserMemoryContext().setBytes(0);

            operator.finish();

            assertEquals(operator.getState(), LOOKUP_SOURCE_BUILT);
            assertFalse(operator.isFinished());
            assertThat(whenBuildFinishes).isDone();
            assertThat(operator.isBlocked()).isNotDone();

            lookupSourceFactory.destroy();
            assertThat(operator.isBlocked()).isDone();

            assertEquals(operator.getState(), LOOKUP_SOURCE_BUILT);

            operator.finish();

            assertEquals(operator.getState(), CLOSED);
            assertTrue(operator.isFinished());
        }
        finally {
            operatorContext.destroy();
        }
    }

    private static Page somePage(List<Type> types)
    {
        int[] initialValues = new int[types.size()];
        Arrays.setAll(initialValues, i -> 100 * i);
        return createSequencePage(types, 7, initialValues);
    }
}
