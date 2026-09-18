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
import io.trino.connector.CatalogServiceProvider;
import io.trino.connector.TestingColumnHandle;
import io.trino.execution.TestingPageSourceProvider;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.metadata.Split;
import io.trino.operator.TableScanOperator.TableScanOperatorFactory;
import io.trino.split.PageSourceManager;
import io.trino.split.PageSourceProvider;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.TestingSplit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.testing.TestingHandles.TEST_TABLE_HANDLE;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestTableScanOperator
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
    public void testSharedMemoryReleasedWithLastReference()
            throws Exception
    {
        AggregatedMemoryContext scanMemoryContext = newSimpleAggregatedMemoryContext();
        PageSourceProvider pageSourceProvider = createPageSourceProvider(scanMemoryContext);
        TableScanOperatorFactory factory = new TableScanOperatorFactory(
                0,
                new PlanNodeId("test"),
                new PlanNodeId("0"),
                (_, _) -> pageSourceProvider,
                TEST_TABLE_HANDLE,
                Optional.empty(),
                ImmutableList.of(new TestingColumnHandle("col0")),
                ImmutableList.of(BIGINT),
                newSimpleAggregatedMemoryContext());

        SourceOperator operator = factory.createOperator(newDriverContext());
        operator.addSplit(new Split(TEST_CATALOG_HANDLE, TestingSplit.createLocalSplit()));
        operator.noMoreSplits();
        while (!operator.isFinished()) {
            operator.getOutput();
        }
        assertThat(scanMemoryContext.getBytes()).isEqualTo(1024);

        operator.close();
        assertThat(scanMemoryContext.getBytes()).isEqualTo(1024);

        factory.noMoreOperators();
        assertThat(scanMemoryContext.getBytes()).isEqualTo(0);
    }

    private static PageSourceProvider createPageSourceProvider(AggregatedMemoryContext scanMemoryContext)
    {
        return new PageSourceManager(CatalogServiceProvider.singleton(TEST_CATALOG_HANDLE, memoryContext -> {
            memoryContext.setBytes(1024);
            return new TestingPageSourceProvider();
        }))
                .createPageSourceProvider(TEST_CATALOG_HANDLE, scanMemoryContext);
    }

    private DriverContext newDriverContext()
    {
        return createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }
}
