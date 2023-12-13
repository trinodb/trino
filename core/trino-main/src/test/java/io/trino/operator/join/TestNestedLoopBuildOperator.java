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
package io.trino.operator.join;

import com.google.common.collect.ImmutableList;
import io.trino.operator.DriverContext;
import io.trino.operator.TaskContext;
import io.trino.operator.join.NestedLoopBuildOperator.NestedLoopBuildOperatorFactory;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.TestingTaskContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestNestedLoopBuildOperator
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;

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
    public void testNestedLoopBuild()
            throws Exception
    {
        TaskContext taskContext = createTaskContext();
        List<Type> buildTypes = ImmutableList.of(BIGINT);
        JoinBridgeManager<NestedLoopJoinBridge> nestedLoopJoinBridgeManager = new JoinBridgeManager<>(
                false,
                new NestedLoopJoinPagesSupplier(),
                buildTypes);
        NestedLoopBuildOperatorFactory nestedLoopBuildOperatorFactory = new NestedLoopBuildOperatorFactory(3, new PlanNodeId("test"), nestedLoopJoinBridgeManager);
        DriverContext driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
        NestedLoopBuildOperator nestedLoopBuildOperator = (NestedLoopBuildOperator) nestedLoopBuildOperatorFactory.createOperator(driverContext);
        NestedLoopJoinBridge nestedLoopJoinBridge = nestedLoopJoinBridgeManager.getJoinBridge();

        assertThat(nestedLoopJoinBridge.getPagesFuture().isDone()).isFalse();

        // build pages
        Page buildPage1 = new Page(3, createLongSequenceBlock(11, 14));
        Page buildPageEmpty = new Page(0);
        Page buildPage2 = new Page(3000, createLongSequenceBlock(4000, 7000));

        nestedLoopBuildOperator.addInput(buildPage1);
        nestedLoopBuildOperator.addInput(buildPageEmpty);
        nestedLoopBuildOperator.addInput(buildPage2);
        nestedLoopBuildOperator.finish();

        assertThat(nestedLoopJoinBridge.getPagesFuture().isDone()).isTrue();
        List<Page> buildPages = nestedLoopJoinBridge.getPagesFuture().get().getPages();

        assertThat(buildPages.get(0)).isEqualTo(buildPage1);
        assertThat(buildPages.get(1)).isEqualTo(buildPage2);
        assertThat(buildPages.size()).isEqualTo(2);
    }

    @Test
    public void testNestedLoopBuildNoBlock()
            throws Exception
    {
        TaskContext taskContext = createTaskContext();
        List<Type> buildTypes = ImmutableList.of();
        JoinBridgeManager<NestedLoopJoinBridge> nestedLoopJoinBridgeManager = new JoinBridgeManager<>(
                false,
                new NestedLoopJoinPagesSupplier(),
                buildTypes);
        NestedLoopBuildOperatorFactory nestedLoopBuildOperatorFactory = new NestedLoopBuildOperatorFactory(3, new PlanNodeId("test"), nestedLoopJoinBridgeManager);
        DriverContext driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
        NestedLoopBuildOperator nestedLoopBuildOperator = (NestedLoopBuildOperator) nestedLoopBuildOperatorFactory.createOperator(driverContext);
        NestedLoopJoinBridge nestedLoopJoinBridge = nestedLoopJoinBridgeManager.getJoinBridge();

        assertThat(nestedLoopJoinBridge.getPagesFuture().isDone()).isFalse();

        // build pages
        Page buildPage1 = new Page(3);
        Page buildPageEmpty = new Page(0);
        Page buildPage2 = new Page(3000);

        nestedLoopBuildOperator.addInput(buildPage1);
        nestedLoopBuildOperator.addInput(buildPageEmpty);
        nestedLoopBuildOperator.addInput(buildPage2);
        nestedLoopBuildOperator.finish();

        assertThat(nestedLoopJoinBridge.getPagesFuture().isDone()).isTrue();
        List<Page> buildPages = nestedLoopJoinBridge.getPagesFuture().get().getPages();

        assertThat(buildPages.size()).isEqualTo(1);
        assertThat(buildPages.get(0).getPositionCount()).isEqualTo(3003);
    }

    @Test
    public void testNestedLoopNoBlocksMaxSizeLimit()
            throws Exception
    {
        TaskContext taskContext = createTaskContext();
        List<Type> buildTypes = ImmutableList.of();
        JoinBridgeManager<NestedLoopJoinBridge> nestedLoopJoinBridgeManager = new JoinBridgeManager<>(
                false,
                new NestedLoopJoinPagesSupplier(),
                buildTypes);
        NestedLoopBuildOperatorFactory nestedLoopBuildOperatorFactory = new NestedLoopBuildOperatorFactory(3, new PlanNodeId("test"), nestedLoopJoinBridgeManager);
        DriverContext driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
        NestedLoopBuildOperator nestedLoopBuildOperator = (NestedLoopBuildOperator) nestedLoopBuildOperatorFactory.createOperator(driverContext);
        NestedLoopJoinBridge nestedLoopJoinBridge = nestedLoopJoinBridgeManager.getJoinBridge();

        assertThat(nestedLoopJoinBridge.getPagesFuture().isDone()).isFalse();

        // build pages
        Page massivePage = new Page(PageProcessor.MAX_BATCH_SIZE + 100);

        nestedLoopBuildOperator.addInput(massivePage);
        nestedLoopBuildOperator.finish();

        assertThat(nestedLoopJoinBridge.getPagesFuture().isDone()).isTrue();
        List<Page> buildPages = nestedLoopJoinBridge.getPagesFuture().get().getPages();
        assertThat(buildPages.size()).isEqualTo(2);
        assertThat(buildPages.get(0).getPositionCount()).isEqualTo(PageProcessor.MAX_BATCH_SIZE);
        assertThat(buildPages.get(1).getPositionCount()).isEqualTo(100);
    }

    private TaskContext createTaskContext()
    {
        return TestingTaskContext.createTaskContext(executor, scheduledExecutor, TEST_SESSION);
    }
}
