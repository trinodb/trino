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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.stats.GcMonitor;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.execution.TaskId;
import io.trino.execution.TaskStateMachine;
import io.trino.memory.context.MemoryReservationHandler;
import io.trino.memory.context.MemoryTrackingContext;
import io.trino.operator.TaskContext;
import io.trino.spi.QueryId;
import io.trino.spiller.SpillSpaceTracker;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static io.trino.ExceededMemoryLimitException.exceededLocalUserMemoryLimit;
import static io.trino.ExceededSpillLimitException.exceededPerQueryLocalLimit;
import static io.trino.memory.context.AggregatedMemoryContext.newRootAggregatedMemoryContext;
import static io.trino.operator.Operator.NOT_BLOCKED;
import static io.trino.operator.TaskContext.createTaskContext;
import static java.lang.String.format;
import static java.util.Map.Entry.comparingByValue;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

@ThreadSafe
public class QueryContext
{
    private static final long GUARANTEED_MEMORY = DataSize.of(1, MEGABYTE).toBytes();

    private final QueryId queryId;
    private final GcMonitor gcMonitor;
    private final Executor notificationExecutor;
    private final ScheduledExecutorService yieldExecutor;
    private final long maxSpill;
    private final SpillSpaceTracker spillSpaceTracker;
    private final Map<TaskId, TaskContext> taskContexts = new ConcurrentHashMap<>();

    private volatile boolean memoryLimitsInitialized;

    // TODO: This field should be final. However, due to the way QueryContext is constructed the memory limit is not known in advance
    @GuardedBy("this")
    private long maxUserMemory;

    private final MemoryTrackingContext queryMemoryContext;
    private final MemoryPool memoryPool;

    @GuardedBy("this")
    private long spillUsed;

    public QueryContext(
            QueryId queryId,
            DataSize maxUserMemory,
            MemoryPool memoryPool,
            GcMonitor gcMonitor,
            Executor notificationExecutor,
            ScheduledExecutorService yieldExecutor,
            DataSize maxSpill,
            SpillSpaceTracker spillSpaceTracker)
    {
        this(
                queryId,
                maxUserMemory,
                memoryPool,
                GUARANTEED_MEMORY,
                gcMonitor,
                notificationExecutor,
                yieldExecutor,
                maxSpill,
                spillSpaceTracker);
    }

    public QueryContext(
            QueryId queryId,
            DataSize maxUserMemory,
            MemoryPool memoryPool,
            long guaranteedMemory,
            GcMonitor gcMonitor,
            Executor notificationExecutor,
            ScheduledExecutorService yieldExecutor,
            DataSize maxSpill,
            SpillSpaceTracker spillSpaceTracker)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.maxUserMemory = requireNonNull(maxUserMemory, "maxUserMemory is null").toBytes();
        this.memoryPool = requireNonNull(memoryPool, "memoryPool is null");
        this.gcMonitor = requireNonNull(gcMonitor, "gcMonitor is null");
        this.notificationExecutor = requireNonNull(notificationExecutor, "notificationExecutor is null");
        this.yieldExecutor = requireNonNull(yieldExecutor, "yieldExecutor is null");
        this.maxSpill = requireNonNull(maxSpill, "maxSpill is null").toBytes();
        this.spillSpaceTracker = requireNonNull(spillSpaceTracker, "spillSpaceTracker is null");
        this.queryMemoryContext = new MemoryTrackingContext(
                newRootAggregatedMemoryContext(new QueryMemoryReservationHandler(this::updateUserMemory, this::tryUpdateUserMemory), guaranteedMemory),
                newRootAggregatedMemoryContext(new QueryMemoryReservationHandler(this::updateRevocableMemory, this::tryReserveMemoryNotSupported), 0L));
    }

    public boolean isMemoryLimitsInitialized()
    {
        return memoryLimitsInitialized;
    }

    // TODO: This method should be removed, and the correct limit set in the constructor. However, due to the way QueryContext is constructed the memory limit is not known in advance
    public synchronized void initializeMemoryLimits(long maxUserMemory)
    {
        checkArgument(maxUserMemory >= 0, "maxUserMemory must be >= 0, found: %s", maxUserMemory);
        this.maxUserMemory = maxUserMemory;
        memoryLimitsInitialized = true;
    }

    @VisibleForTesting
    MemoryTrackingContext getQueryMemoryContext()
    {
        return queryMemoryContext;
    }

    @VisibleForTesting
    public synchronized long getMaxUserMemory()
    {
        return maxUserMemory;
    }

    public QueryId getQueryId()
    {
        return queryId;
    }

    private synchronized ListenableFuture<Void> updateUserMemory(String allocationTag, long delta)
    {
        if (delta >= 0) {
            enforceUserMemoryLimit(queryMemoryContext.getUserMemory(), delta, maxUserMemory);
            ListenableFuture<Void> future = memoryPool.reserve(queryId, allocationTag, delta);
            if (future.isDone()) {
                return NOT_BLOCKED;
            }

            return future;
        }
        memoryPool.free(queryId, allocationTag, -delta);
        return NOT_BLOCKED;
    }

    //TODO Add tagging support for revocable memory reservations if needed
    private synchronized ListenableFuture<Void> updateRevocableMemory(String allocationTag, long delta)
    {
        if (delta >= 0) {
            ListenableFuture<Void> future = memoryPool.reserveRevocable(queryId, delta);
            if (future.isDone()) {
                return NOT_BLOCKED;
            }

            return future;
        }
        memoryPool.freeRevocable(queryId, -delta);
        return NOT_BLOCKED;
    }

    //TODO move spill tracking to the new memory tracking framework
    public synchronized ListenableFuture<Void> reserveSpill(long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");
        if (spillUsed + bytes > maxSpill) {
            throw exceededPerQueryLocalLimit(succinctBytes(maxSpill));
        }
        ListenableFuture<Void> future = spillSpaceTracker.reserve(bytes);
        spillUsed += bytes;
        return future;
    }

    private synchronized boolean tryUpdateUserMemory(String allocationTag, long delta)
    {
        if (delta <= 0) {
            ListenableFuture<Void> future = updateUserMemory(allocationTag, delta);
            // When delta == 0 and the pool is full the future can still not be done,
            // but, for negative deltas it must always be done.
            if (delta < 0) {
                verify(future.isDone(), "future should be done");
            }
            return true;
        }
        if (queryMemoryContext.getUserMemory() + delta > maxUserMemory) {
            return false;
        }
        return memoryPool.tryReserve(queryId, allocationTag, delta);
    }

    public synchronized void freeSpill(long bytes)
    {
        checkArgument(spillUsed - bytes >= 0, "tried to free more memory than is reserved");
        spillUsed -= bytes;
        spillSpaceTracker.free(bytes);
    }

    public synchronized MemoryPool getMemoryPool()
    {
        return memoryPool;
    }

    public TaskContext addTaskContext(
            TaskStateMachine taskStateMachine,
            Session session,
            Runnable notifyStatusChanged,
            boolean perOperatorCpuTimerEnabled,
            boolean cpuTimerEnabled)
    {
        TaskContext taskContext = createTaskContext(
                this,
                taskStateMachine,
                gcMonitor,
                notificationExecutor,
                yieldExecutor,
                session,
                queryMemoryContext.newMemoryTrackingContext(),
                notifyStatusChanged,
                perOperatorCpuTimerEnabled,
                cpuTimerEnabled);
        taskContexts.put(taskStateMachine.getTaskId(), taskContext);
        return taskContext;
    }

    public <C, R> R accept(QueryContextVisitor<C, R> visitor, C context)
    {
        return visitor.visitQueryContext(this, context);
    }

    public <C, R> List<R> acceptChildren(QueryContextVisitor<C, R> visitor, C context)
    {
        return taskContexts.values()
                .stream()
                .map(taskContext -> taskContext.accept(visitor, context))
                .collect(toList());
    }

    public TaskContext getTaskContextByTaskId(TaskId taskId)
    {
        TaskContext taskContext = taskContexts.get(taskId);
        return verifyNotNull(taskContext, "task does not exist");
    }

    private static class QueryMemoryReservationHandler
            implements MemoryReservationHandler
    {
        private final BiFunction<String, Long, ListenableFuture<Void>> reserveMemoryFunction;
        private final BiPredicate<String, Long> tryReserveMemoryFunction;

        public QueryMemoryReservationHandler(
                BiFunction<String, Long, ListenableFuture<Void>> reserveMemoryFunction,
                BiPredicate<String, Long> tryReserveMemoryFunction)
        {
            this.reserveMemoryFunction = requireNonNull(reserveMemoryFunction, "reserveMemoryFunction is null");
            this.tryReserveMemoryFunction = requireNonNull(tryReserveMemoryFunction, "tryReserveMemoryFunction is null");
        }

        @Override
        public ListenableFuture<Void> reserveMemory(String allocationTag, long delta)
        {
            return reserveMemoryFunction.apply(allocationTag, delta);
        }

        @Override
        public boolean tryReserveMemory(String allocationTag, long delta)
        {
            return tryReserveMemoryFunction.test(allocationTag, delta);
        }
    }

    private boolean tryReserveMemoryNotSupported(String allocationTag, long bytes)
    {
        throw new UnsupportedOperationException("tryReserveMemory is not supported");
    }

    @GuardedBy("this")
    private void enforceUserMemoryLimit(long allocated, long delta, long maxMemory)
    {
        if (allocated + delta > maxMemory) {
            throw exceededLocalUserMemoryLimit(succinctBytes(maxMemory), getAdditionalFailureInfo(allocated, delta));
        }
    }

    @GuardedBy("this")
    private String getAdditionalFailureInfo(long allocated, long delta)
    {
        Map<String, Long> queryAllocations = memoryPool.getTaggedMemoryAllocations().get(queryId);

        String additionalInfo = format("Allocated: %s, Delta: %s", succinctBytes(allocated), succinctBytes(delta));

        // It's possible that a query tries allocating more than the available memory
        // failing immediately before any allocation of that query is tagged
        if (queryAllocations == null) {
            return additionalInfo;
        }

        String topConsumers = queryAllocations.entrySet().stream()
                .sorted(comparingByValue(Comparator.reverseOrder()))
                .limit(3)
                .filter(e -> e.getValue() >= 0)
                .collect(toImmutableMap(Entry::getKey, e -> succinctBytes(e.getValue())))
                .toString();

        return format("%s, Top Consumers: %s", additionalInfo, topConsumers);
    }
}
