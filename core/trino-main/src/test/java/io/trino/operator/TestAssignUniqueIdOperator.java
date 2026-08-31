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

import io.trino.RowPagesBuilder;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.sql.planner.plan.PlanNodeId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.OperatorAssertion.toPages;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestAssignUniqueIdOperator
{
    private static final int ROWS_PER_PAGE = 1000;
    private static final int PAGE_COUNT = 10;
    private static final int TOTAL_ROWS = ROWS_PER_PAGE * PAGE_COUNT;

    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
    private final ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));

    @AfterAll
    void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    void testAssignUniqueIds()
    {
        OperatorFactory operatorFactory = AssignUniqueIdOperator.createOperatorFactory(0, new PlanNodeId("test"), new AtomicLong());
        TaskContext taskContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION);

        Set<Long> ids = new HashSet<>();
        collectIds(operatorFactory, newDriverContext(taskContext, 0), ids);

        assertThat(ids).hasSize(TOTAL_ROWS);
    }

    @Test
    void testFactoriesSharingValuePoolProduceDistinctIds()
    {
        // Two AssignUniqueId nodes executing in the same task (e.g. the MERGE target and source
        // sides of a colocated join) must not produce the same id for different rows
        AtomicLong valuePool = new AtomicLong();
        OperatorFactory targetFactory = AssignUniqueIdOperator.createOperatorFactory(0, new PlanNodeId("target"), valuePool);
        OperatorFactory sourceFactory = AssignUniqueIdOperator.createOperatorFactory(1, new PlanNodeId("source"), valuePool);

        // Both operators run in the same task, so the id mask is identical for the two sides
        // and only the shared pool keeps their ids apart
        TaskContext taskContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION);

        Set<Long> ids = new HashSet<>();
        collectIds(targetFactory, newDriverContext(taskContext, 0), ids);
        collectIds(sourceFactory, newDriverContext(taskContext, 1), ids);

        assertThat(ids).hasSize(2 * TOTAL_ROWS);
    }

    @Test
    void testDuplicatedFactoriesProduceDistinctIds()
    {
        // Factories duplicated for additional pipelines (e.g. lookup outer drivers) share the pool
        OperatorFactory operatorFactory = AssignUniqueIdOperator.createOperatorFactory(0, new PlanNodeId("test"), new AtomicLong());
        OperatorFactory duplicateFactory = operatorFactory.duplicate();
        TaskContext taskContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION);

        Set<Long> ids = new HashSet<>();
        collectIds(operatorFactory, newDriverContext(taskContext, 0), ids);
        collectIds(duplicateFactory, newDriverContext(taskContext, 1), ids);

        assertThat(ids).hasSize(2 * TOTAL_ROWS);
    }

    private static void collectIds(OperatorFactory operatorFactory, DriverContext driverContext, Set<Long> ids)
    {
        RowPagesBuilder pagesBuilder = rowPagesBuilder(BIGINT);
        for (int page = 0; page < PAGE_COUNT; page++) {
            pagesBuilder.addSequencePage(ROWS_PER_PAGE, 0);
        }
        List<Page> input = pagesBuilder.build();

        List<Page> output = toPages(operatorFactory, driverContext, input);
        for (Page page : output) {
            assertThat(page.getChannelCount()).isEqualTo(2);
            Block idBlock = page.getBlock(1);
            for (int position = 0; position < page.getPositionCount(); position++) {
                assertThat(ids.add(BIGINT.getLong(idBlock, position))).isTrue();
            }
        }
    }

    private static DriverContext newDriverContext(TaskContext taskContext, int pipelineId)
    {
        return taskContext
                .addPipelineContext(pipelineId, true, true, false)
                .addDriverContext();
    }
}
