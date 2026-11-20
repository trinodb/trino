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
package io.trino.operator.function;

import com.google.common.collect.ImmutableList;
import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.PagesIndex;
import io.trino.operator.function.TableFunctionOperator.TableFunctionOperatorFactory;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionDataProcessor;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.function.table.TableFunctionProcessorState;
import io.trino.sql.planner.plan.PlanNodeId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.OperatorAssertion.toPages;
import static io.trino.spi.function.table.TableFunctionProcessorState.Blocked.blocked;
import static io.trino.spi.function.table.TableFunctionProcessorState.Finished.FINISHED;
import static io.trino.spi.function.table.TableFunctionProcessorState.Processed.usedInput;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestTableFunctionOperator
{
    private ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
    private ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));

    @AfterAll
    public void tearDown()
    {
        executor.shutdownNow();
        executor = null;
        scheduledExecutor.shutdownNow();
        scheduledExecutor = null;
    }

    @Test
    public void testDataProcessorClosedForEachPartition()
            throws Exception
    {
        List<TestingDataProcessor> processors = new ArrayList<>();
        Operator operator = createOperator(() -> {
            TestingDataProcessor processor = new TestingDataProcessor(false);
            processors.add(processor);
            return processor;
        });

        // two partitions
        List<Page> input = rowPagesBuilder(BIGINT)
                .row(1L)
                .row(1L)
                .row(2L)
                .row(2L)
                .build();
        assertThat(toPages(operator, input.iterator())).isEmpty();
        assertThat(processors).hasSize(2);
        assertThat(processors.get(0).closeCount).isEqualTo(1);
        assertThat(processors.get(1).closeCount).isEqualTo(0);

        operator.close();
        assertThat(processors.get(0).closeCount).isEqualTo(1);
        assertThat(processors.get(1).closeCount).isEqualTo(1);
    }

    @Test
    public void testDataProcessorClosedWhenOperatorClosedBeforeFinish()
            throws Exception
    {
        List<TestingDataProcessor> processors = new ArrayList<>();
        Operator operator = createOperator(() -> {
            TestingDataProcessor processor = new TestingDataProcessor(true);
            processors.add(processor);
            return processor;
        });

        operator.addInput(rowPagesBuilder(BIGINT)
                .row(1L)
                .buildPage());
        operator.finish();
        assertThat(operator.getOutput()).isNull();
        assertThat(operator.isFinished()).isFalse();
        assertThat(processors).hasSize(1);
        assertThat(processors.getFirst().closeCount).isEqualTo(0);

        operator.close();
        assertThat(processors.getFirst().closeCount).isEqualTo(1);
    }

    @Test
    public void testDataProcessorMemoryAccounted()
            throws Exception
    {
        List<MemoryContext> memoryContexts = new ArrayList<>();
        Operator operator = createOperator(memoryContext -> {
            memoryContexts.add(memoryContext);
            return new TestingDataProcessor(true);
        });
        DriverContext driverContext = operator.getOperatorContext().getDriverContext();

        operator.addInput(rowPagesBuilder(BIGINT)
                .row(1L)
                .buildPage());
        operator.finish();
        assertThat(operator.getOutput()).isNull();
        assertThat(memoryContexts).hasSize(1);

        // the operator also accounts for the buffered input
        long memoryUsage = driverContext.getMemoryUsage();
        memoryContexts.get(0).setBytes(1024);
        assertThat(driverContext.getMemoryUsage()).isEqualTo(memoryUsage + 1024);

        operator.close();
        assertThat(driverContext.getMemoryUsage()).isEqualTo(memoryUsage);
    }

    @Test
    public void testDataProcessorMemoryReleasedWithPartition()
            throws Exception
    {
        List<MemoryContext> memoryContexts = new ArrayList<>();
        Operator operator = createOperator(memoryContext -> {
            // the processor of the second partition does not report memory
            if (memoryContexts.isEmpty()) {
                memoryContext.setBytes(1024);
            }
            memoryContexts.add(memoryContext);
            return new TestingDataProcessor(false);
        });
        DriverContext driverContext = operator.getOperatorContext().getDriverContext();

        // two partitions
        List<Page> input = rowPagesBuilder(BIGINT)
                .row(1L)
                .row(2L)
                .build();
        assertThat(toPages(operator, input.iterator())).isEmpty();
        assertThat(memoryContexts).hasSize(2);
        assertThat(driverContext.getMemoryUsage()).isEqualTo(0);

        operator.close();
    }

    private Operator createOperator(Supplier<TableFunctionDataProcessor> processorSupplier)
    {
        return createOperator(_ -> processorSupplier.get());
    }

    private Operator createOperator(Function<MemoryContext, TableFunctionDataProcessor> processorFactory)
    {
        TableFunctionProcessorProvider provider = new TableFunctionProcessorProvider()
        {
            @Override
            public TableFunctionDataProcessor getDataProcessor(ConnectorSession session, ConnectorTableFunctionHandle handle, MemoryContext memoryContext)
            {
                return processorFactory.apply(memoryContext);
            }
        };
        return new TableFunctionOperatorFactory(
                0,
                new PlanNodeId("test"),
                provider,
                TEST_CATALOG_HANDLE,
                new ConnectorTableFunctionHandle() {},
                0,
                0,
                ImmutableList.of(ImmutableList.of(0)),
                Optional.empty(),
                ImmutableList.of(),
                true,
                ImmutableList.of(0),
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableList.of(),
                0,
                ImmutableList.of(BIGINT),
                10,
                new PagesIndex.TestingFactory(false))
                .createOperator(newDriverContext());
    }

    private DriverContext newDriverContext()
    {
        return createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }

    private static class TestingDataProcessor
            implements TableFunctionDataProcessor
    {
        private final boolean blocked;
        private int closeCount;

        TestingDataProcessor(boolean blocked)
        {
            this.blocked = blocked;
        }

        @Override
        public TableFunctionProcessorState process(List<Optional<Page>> input)
        {
            if (blocked) {
                return blocked(new CompletableFuture<>());
            }
            if (input == null) {
                return FINISHED;
            }
            return usedInput();
        }

        @Override
        public void close()
        {
            closeCount++;
        }
    }
}
