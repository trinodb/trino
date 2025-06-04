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
package io.trino.memory;

import io.airlift.stats.TestingGcMonitor;
import io.airlift.units.DataSize;
import io.trino.ExceededMemoryLimitException;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.execution.TaskStateMachine;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.memory.context.MemoryTrackingContext;
import io.trino.operator.DriverContext;
import io.trino.operator.DriverStats;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorStats;
import io.trino.operator.PipelineContext;
import io.trino.operator.PipelineStats;
import io.trino.operator.TaskContext;
import io.trino.operator.TaskStats;
import io.trino.spi.QueryId;
import io.trino.spiller.SpillSpaceTracker;
import io.trino.sql.planner.plan.PlanNodeId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestMemoryTracking
{
    private static final DataSize queryMaxMemory = DataSize.of(1, GIGABYTE);
    private static final DataSize memoryPoolSize = DataSize.of(1, GIGABYTE);
    private static final DataSize maxSpillSize = DataSize.of(1, GIGABYTE);
    private static final DataSize queryMaxSpillSize = DataSize.of(1, GIGABYTE);
    private static final SpillSpaceTracker spillSpaceTracker = new SpillSpaceTracker(maxSpillSize);

    private QueryContext queryContext;
    private TaskContext taskContext;
    private PipelineContext pipelineContext;
    private DriverContext driverContext;
    private OperatorContext operatorContext;
    private MemoryPool memoryPool;
    private ExecutorService notificationExecutor;
    private ScheduledExecutorService yieldExecutor;
    private ScheduledExecutorService timeoutExecutor;

    @AfterEach
    public void tearDown()
    {
        notificationExecutor.shutdownNow();
        timeoutExecutor.shutdownNow();
        yieldExecutor.shutdownNow();
        queryContext = null;
        taskContext = null;
        pipelineContext = null;
        driverContext = null;
        operatorContext = null;
        memoryPool = null;
    }

    @BeforeEach
    public void setUpTest()
    {
        notificationExecutor = newCachedThreadPool(daemonThreadsNamed("local-query-runner-executor-%s"));
        yieldExecutor = newScheduledThreadPool(2, daemonThreadsNamed("local-query-runner-scheduler-%s"));
        timeoutExecutor = newScheduledThreadPool(2, daemonThreadsNamed("local-query-runner-driver-timeout-%s"));

        memoryPool = new MemoryPool(memoryPoolSize);
        queryContext = new QueryContext(
                new QueryId("test_query"),
                queryMaxMemory,
                memoryPool,
                new TestingGcMonitor(),
                notificationExecutor,
                yieldExecutor,
                timeoutExecutor,
                queryMaxSpillSize,
                spillSpaceTracker);
        taskContext = queryContext.addTaskContext(
                new TaskStateMachine(new TaskId(new StageId("test_query", 0), 0, 0), notificationExecutor),
                testSessionBuilder().build(),
                () -> {},
                true,
                true);
        pipelineContext = taskContext.addPipelineContext(0, true, true, false);
        driverContext = pipelineContext.addDriverContext();
        operatorContext = driverContext.addOperatorContext(1, new PlanNodeId("a"), "test-operator");
    }

    @Test
    public void testOperatorAllocations()
    {
        MemoryTrackingContext operatorMemoryContext = operatorContext.getOperatorMemoryContext();
        LocalMemoryContext userMemory = operatorContext.localUserMemoryContext();
        LocalMemoryContext revocableMemory = operatorContext.localRevocableMemoryContext();
        userMemory.setBytes(100);
        assertOperatorMemoryAllocations(operatorMemoryContext, 100, 0);
        assertOperatorMemoryAllocations(operatorMemoryContext, 100, 0);
        userMemory.setBytes(500);
        assertOperatorMemoryAllocations(operatorMemoryContext, 500, 0);
        userMemory.setBytes(userMemory.getBytes() - 500);
        assertOperatorMemoryAllocations(operatorMemoryContext, 0, 0);
        revocableMemory.setBytes(300);
        assertOperatorMemoryAllocations(operatorMemoryContext, 0, 300);
        assertThatThrownBy(() -> userMemory.setBytes(userMemory.getBytes() - 500))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("bytes cannot be negative");
        operatorContext.destroy();
        assertOperatorMemoryAllocations(operatorMemoryContext, 0, 0);
    }

    @Test
    public void testLocalTotalMemoryLimitExceeded()
    {
        LocalMemoryContext memoryContext = operatorContext.newLocalUserMemoryContext("test");
        memoryContext.setBytes(100);
        assertOperatorMemoryAllocations(operatorContext.getOperatorMemoryContext(), 100, 0);
        memoryContext.setBytes(queryMaxMemory.toBytes());
        assertOperatorMemoryAllocations(operatorContext.getOperatorMemoryContext(), queryMaxMemory.toBytes(), 0);
        assertThatThrownBy(() -> memoryContext.setBytes(queryMaxMemory.toBytes() + 1))
                .isInstanceOf(ExceededMemoryLimitException.class)
                .hasMessage("Query exceeded per-node memory limit of %1$s [Allocated: %1$s, Delta: 1B, Top Consumers: {test=%1$s}]", queryMaxMemory);
    }

    @Test
    public void testLocalAllocations()
    {
        long pipelineLocalAllocation = 1_000_000;
        long taskLocalAllocation = 10_000_000;
        LocalMemoryContext pipelineLocalMemoryContext = pipelineContext.localMemoryContext();
        pipelineLocalMemoryContext.setBytes(pipelineLocalAllocation);
        assertLocalMemoryAllocations(pipelineContext.getPipelineMemoryContext(),
                pipelineLocalAllocation,
                1_000_000);
        LocalMemoryContext taskLocalMemoryContext = taskContext.localMemoryContext();
        taskLocalMemoryContext.setBytes(taskLocalAllocation);
        assertLocalMemoryAllocations(
                taskContext.getTaskMemoryContext(),
                pipelineLocalAllocation + taskLocalAllocation,
                11_000_000);
        assertThat(pipelineContext.getPipelineStats().getUserMemoryReservation().toBytes())
                .describedAs("task level allocations should not be visible at the pipeline level")
                .isEqualTo(pipelineLocalAllocation);
        pipelineLocalMemoryContext.setBytes(pipelineLocalMemoryContext.getBytes() - pipelineLocalAllocation);
        assertLocalMemoryAllocations(
                pipelineContext.getPipelineMemoryContext(),
                taskLocalAllocation,
                0);
        taskLocalMemoryContext.setBytes(taskLocalMemoryContext.getBytes() - taskLocalAllocation);
        assertLocalMemoryAllocations(
                taskContext.getTaskMemoryContext(),
                0,
                0);
    }

    @Test
    public void testStats()
    {
        LocalMemoryContext userMemory = operatorContext.localUserMemoryContext();
        userMemory.setBytes(100_000_000);

        assertStats(
                operatorContext.getOperatorStats(),
                driverContext.getDriverStats(),
                pipelineContext.getPipelineStats(),
                taskContext.getTaskStats(),
                100_000_000,
                0);

        // allocate more and check peak memory reservation
        userMemory.setBytes(600_000_000);
        assertStats(
                operatorContext.getOperatorStats(),
                driverContext.getDriverStats(),
                pipelineContext.getPipelineStats(),
                taskContext.getTaskStats(),
                600_000_000,
                0);

        userMemory.setBytes(userMemory.getBytes() - 300_000_000);
        assertStats(
                operatorContext.getOperatorStats(),
                driverContext.getDriverStats(),
                pipelineContext.getPipelineStats(),
                taskContext.getTaskStats(),
                300_000_000,
                0);

        userMemory.setBytes(userMemory.getBytes() - 300_000_000);
        assertStats(
                operatorContext.getOperatorStats(),
                driverContext.getDriverStats(),
                pipelineContext.getPipelineStats(),
                taskContext.getTaskStats(),
                0,
                0);

        operatorContext.destroy();

        assertStats(
                operatorContext.getOperatorStats(),
                driverContext.getDriverStats(),
                pipelineContext.getPipelineStats(),
                taskContext.getTaskStats(),
                0,
                0);
    }

    @Test
    public void testRevocableMemoryAllocations()
    {
        LocalMemoryContext userMemory = operatorContext.localUserMemoryContext();
        LocalMemoryContext revocableMemory = operatorContext.localRevocableMemoryContext();
        revocableMemory.setBytes(100_000_000);
        assertStats(
                operatorContext.getOperatorStats(),
                driverContext.getDriverStats(),
                pipelineContext.getPipelineStats(),
                taskContext.getTaskStats(),
                0,
                100_000_000);
        userMemory.setBytes(100_000_000);
        revocableMemory.setBytes(200_000_000);
        assertStats(
                operatorContext.getOperatorStats(),
                driverContext.getDriverStats(),
                pipelineContext.getPipelineStats(),
                taskContext.getTaskStats(),
                100_000_000,
                200_000_000);
    }

    @Test
    public void testTrySetBytes()
    {
        LocalMemoryContext localMemoryContext = operatorContext.localUserMemoryContext();
        assertThat(localMemoryContext.trySetBytes(100_000_000)).isTrue();
        assertStats(
                operatorContext.getOperatorStats(),
                driverContext.getDriverStats(),
                pipelineContext.getPipelineStats(),
                taskContext.getTaskStats(),
                100_000_000,
                0);

        assertThat(localMemoryContext.trySetBytes(200_000_000)).isTrue();
        assertStats(
                operatorContext.getOperatorStats(),
                driverContext.getDriverStats(),
                pipelineContext.getPipelineStats(),
                taskContext.getTaskStats(),
                200_000_000,
                0);

        assertThat(localMemoryContext.trySetBytes(100_000_000)).isTrue();
        assertStats(
                operatorContext.getOperatorStats(),
                driverContext.getDriverStats(),
                pipelineContext.getPipelineStats(),
                taskContext.getTaskStats(),
                100_000_000,
                0);

        // allocating more than the pool size should fail and we should have the same stats as before
        assertThat(localMemoryContext.trySetBytes(memoryPool.getMaxBytes() + 1)).isFalse();
        assertStats(
                operatorContext.getOperatorStats(),
                driverContext.getDriverStats(),
                pipelineContext.getPipelineStats(),
                taskContext.getTaskStats(),
                100_000_000,
                0);
    }

    @Test
    public void testTrySetZeroBytesFullPool()
    {
        LocalMemoryContext localMemoryContext = operatorContext.localUserMemoryContext();
        // fill up the pool
        TaskId taskId = new TaskId(new StageId("test_query", 0), 0, 0);
        memoryPool.reserve(taskId, "test", memoryPool.getFreeBytes());
        // try to reserve 0 bytes in the full pool
        assertThat(localMemoryContext.trySetBytes(localMemoryContext.getBytes())).isTrue();
    }

    @Test
    public void testDestroy()
    {
        LocalMemoryContext newLocalUserMemoryContext = operatorContext.localUserMemoryContext();
        LocalMemoryContext newLocalRevocableMemoryContext = operatorContext.localRevocableMemoryContext();
        newLocalRevocableMemoryContext.setBytes(200_000);
        newLocalUserMemoryContext.setBytes(400_000);
        assertThat(operatorContext.getOperatorMemoryContext().getUserMemory()).isEqualTo(400_000);
        operatorContext.destroy();
        assertOperatorMemoryAllocations(operatorContext.getOperatorMemoryContext(), 0, 0);
    }

    private void assertStats(
            OperatorStats operatorStats,
            DriverStats driverStats,
            PipelineStats pipelineStats,
            TaskStats taskStats,
            long expectedUserMemory,
            long expectedRevocableMemory)
    {
        assertThat(operatorStats.getUserMemoryReservation().toBytes()).isEqualTo(expectedUserMemory);
        assertThat(driverStats.getUserMemoryReservation().toBytes()).isEqualTo(expectedUserMemory);
        assertThat(pipelineStats.getUserMemoryReservation().toBytes()).isEqualTo(expectedUserMemory);
        assertThat(taskStats.getUserMemoryReservation().toBytes()).isEqualTo(expectedUserMemory);

        assertThat(operatorStats.getRevocableMemoryReservation().toBytes()).isEqualTo(expectedRevocableMemory);
        assertThat(driverStats.getRevocableMemoryReservation().toBytes()).isEqualTo(expectedRevocableMemory);
        assertThat(pipelineStats.getRevocableMemoryReservation().toBytes()).isEqualTo(expectedRevocableMemory);
        assertThat(taskStats.getRevocableMemoryReservation().toBytes()).isEqualTo(expectedRevocableMemory);
    }

    // the allocations that are done at the operator level are reflected at that level and all the way up to the pools
    private void assertOperatorMemoryAllocations(
            MemoryTrackingContext memoryTrackingContext,
            long expectedUserMemory,
            long expectedRevocableMemory)
    {
        assertThat(memoryTrackingContext.getUserMemory())
                .describedAs("User memory verification failed")
                .isEqualTo(expectedUserMemory);
        assertThat(memoryPool.getReservedBytes())
                .describedAs("Memory pool verification failed")
                .isEqualTo(expectedUserMemory);
        assertThat(memoryTrackingContext.getRevocableMemory())
                .describedAs("Revocable memory verification failed")
                .isEqualTo(expectedRevocableMemory);
    }

    // the local allocations are reflected only at that level and all the way up to the pools
    private void assertLocalMemoryAllocations(
            MemoryTrackingContext memoryTrackingContext,
            long expectedPoolMemory,
            long expectedContextUserMemory)
    {
        assertThat(memoryTrackingContext.getUserMemory())
                .describedAs("User memory verification failed")
                .isEqualTo(expectedContextUserMemory);
        assertThat(memoryPool.getReservedBytes())
                .describedAs("Memory pool verification failed")
                .isEqualTo(expectedPoolMemory);
    }
}
