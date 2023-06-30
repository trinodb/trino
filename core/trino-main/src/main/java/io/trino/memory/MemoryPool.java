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
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.trino.execution.TaskId;
import io.trino.spi.QueryId;
import io.trino.spi.memory.MemoryAllocation;
import io.trino.spi.memory.MemoryPoolInfo;
import org.weakref.jmx.Managed;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.operator.Operator.NOT_BLOCKED;
import static java.util.Objects.requireNonNull;

public class MemoryPool
{
    private final long maxBytes;

    @GuardedBy("this")
    private long reservedBytes;
    @GuardedBy("this")
    private long reservedRevocableBytes;

    @Nullable
    @GuardedBy("this")
    private NonCancellableMemoryFuture<Void> future;

    @GuardedBy("this")
    // TODO: It would be better if we just tracked QueryContexts, but their lifecycle is managed by a weak reference, so we can't do that
    private final Map<QueryId, Long> queryMemoryReservations = new HashMap<>();

    // This map keeps track of all the tagged allocations, e.g., query-1 -> ['TableScanOperator': 10MB, 'LazyOutputBuffer': 5MB, ...]
    @GuardedBy("this")
    private final Map<QueryId, Map<String, Long>> taggedMemoryAllocations = new HashMap<>();

    @GuardedBy("this")
    private final Map<QueryId, Long> queryRevocableMemoryReservations = new HashMap<>();

    @GuardedBy("this")
    private final Map<TaskId, Long> taskMemoryReservations = new HashMap<>();

    @GuardedBy("this")
    private final Map<TaskId, Long> taskRevocableMemoryReservations = new HashMap<>();

    private final List<MemoryPoolListener> listeners = new CopyOnWriteArrayList<>();

    public MemoryPool(DataSize size)
    {
        requireNonNull(size, "size is null");
        maxBytes = size.toBytes();
    }

    public synchronized MemoryPoolInfo getInfo()
    {
        Map<QueryId, List<MemoryAllocation>> memoryAllocations = new HashMap<>();
        for (Entry<QueryId, Map<String, Long>> entry : taggedMemoryAllocations.entrySet()) {
            List<MemoryAllocation> allocations = new ArrayList<>();
            if (entry.getValue() != null) {
                entry.getValue().forEach((tag, allocation) -> allocations.add(new MemoryAllocation(tag, allocation)));
            }
            memoryAllocations.put(entry.getKey(), allocations);
        }

        Map<String, Long> stringKeyedTaskMemoryReservations = taskMemoryReservations.entrySet().stream()
                .collect(toImmutableMap(
                        entry -> entry.getKey().toString(),
                        Entry::getValue));

        Map<String, Long> stringKeyedTaskRevocableMemoryReservations = taskRevocableMemoryReservations.entrySet().stream()
                .collect(toImmutableMap(
                        entry -> entry.getKey().toString(),
                        Entry::getValue));

        return new MemoryPoolInfo(
                maxBytes,
                reservedBytes,
                reservedRevocableBytes,
                queryMemoryReservations,
                memoryAllocations,
                queryRevocableMemoryReservations,
                stringKeyedTaskMemoryReservations,
                stringKeyedTaskRevocableMemoryReservations);
    }

    public void addListener(MemoryPoolListener listener)
    {
        listeners.add(requireNonNull(listener, "listener cannot be null"));
    }

    public void removeListener(MemoryPoolListener listener)
    {
        listeners.remove(requireNonNull(listener, "listener cannot be null"));
    }

    /**
     * Reserves the given number of bytes. Caller should wait on the returned future, before allocating more memory.
     */
    public ListenableFuture<Void> reserve(TaskId taskId, String allocationTag, long bytes)
    {
        checkArgument(bytes >= 0, "'%s' is negative", bytes);
        ListenableFuture<Void> result;
        synchronized (this) {
            if (bytes != 0) {
                QueryId queryId = taskId.getQueryId();
                queryMemoryReservations.merge(queryId, bytes, Long::sum);
                updateTaggedMemoryAllocations(queryId, allocationTag, bytes);
                taskMemoryReservations.merge(taskId, bytes, Long::sum);
            }
            reservedBytes += bytes;
            if (getFreeBytes() <= 0) {
                if (future == null) {
                    future = NonCancellableMemoryFuture.create();
                }
                checkState(!future.isDone(), "future is already completed");
                result = future;
            }
            else {
                result = NOT_BLOCKED;
            }
        }

        onMemoryReserved();
        return result;
    }

    private void onMemoryReserved()
    {
        listeners.forEach(listener -> listener.onMemoryReserved(this));
    }

    public ListenableFuture<Void> reserveRevocable(TaskId taskId, long bytes)
    {
        checkArgument(bytes >= 0, "'%s' is negative", bytes);

        ListenableFuture<Void> result;
        synchronized (this) {
            if (bytes != 0) {
                queryRevocableMemoryReservations.merge(taskId.getQueryId(), bytes, Long::sum);
                taskRevocableMemoryReservations.merge(taskId, bytes, Long::sum);
            }
            reservedRevocableBytes += bytes;
            if (getFreeBytes() <= 0) {
                if (future == null) {
                    future = NonCancellableMemoryFuture.create();
                }
                checkState(!future.isDone(), "future is already completed");
                result = future;
            }
            else {
                result = NOT_BLOCKED;
            }
        }

        onMemoryReserved();
        return result;
    }

    /**
     * Try to reserve the given number of bytes. Return value indicates whether the caller may use the requested memory.
     */
    public boolean tryReserve(TaskId taskId, String allocationTag, long bytes)
    {
        checkArgument(bytes >= 0, "'%s' is negative", bytes);
        synchronized (this) {
            if (getFreeBytes() - bytes < 0) {
                return false;
            }
            reservedBytes += bytes;
            if (bytes != 0) {
                QueryId queryId = taskId.getQueryId();
                queryMemoryReservations.merge(queryId, bytes, Long::sum);
                updateTaggedMemoryAllocations(queryId, allocationTag, bytes);
                taskMemoryReservations.merge(taskId, bytes, Long::sum);
            }
        }

        onMemoryReserved();
        return true;
    }

    public boolean tryReserveRevocable(long bytes)
    {
        checkArgument(bytes >= 0, "'%s' is negative", bytes);
        synchronized (this) {
            if (getFreeBytes() - bytes < 0) {
                return false;
            }
            reservedRevocableBytes += bytes;
        }

        onMemoryReserved();
        return true;
    }

    public synchronized void free(TaskId taskId, String allocationTag, long bytes)
    {
        checkArgument(bytes >= 0, "'%s' is negative", bytes);
        checkArgument(reservedBytes >= bytes, "tried to free more memory than is reserved");
        if (bytes == 0) {
            // Freeing zero bytes is a no-op
            return;
        }

        QueryId queryId = taskId.getQueryId();
        Long queryReservation = queryMemoryReservations.get(queryId);
        requireNonNull(queryReservation, "queryReservation is null");
        checkArgument(queryReservation >= bytes, "tried to free more memory than is reserved by query");

        Long taskReservation = taskMemoryReservations.get(taskId);
        requireNonNull(taskReservation, "taskReservation is null");
        checkArgument(taskReservation >= bytes, "tried to free more memory than is reserved by task");

        queryReservation -= bytes;
        if (queryReservation == 0) {
            queryMemoryReservations.remove(queryId);
            taggedMemoryAllocations.remove(queryId);
        }
        else {
            queryMemoryReservations.put(queryId, queryReservation);
            updateTaggedMemoryAllocations(queryId, allocationTag, -bytes);
        }

        taskReservation -= bytes;
        if (taskReservation == 0) {
            taskMemoryReservations.remove(taskId);
        }
        else {
            taskMemoryReservations.put(taskId, taskReservation);
        }

        reservedBytes -= bytes;
        if (getFreeBytes() > 0 && future != null) {
            future.set(null);
            future = null;
        }
    }

    public synchronized void freeRevocable(TaskId taskId, long bytes)
    {
        checkArgument(bytes >= 0, "'%s' is negative", bytes);
        checkArgument(reservedRevocableBytes >= bytes, "tried to free more revocable memory than is reserved");
        if (bytes == 0) {
            // Freeing zero bytes is a no-op
            return;
        }

        QueryId queryId = taskId.getQueryId();
        Long queryReservation = queryRevocableMemoryReservations.get(queryId);
        requireNonNull(queryReservation, "queryReservation is null");
        checkArgument(queryReservation >= bytes, "tried to free more revocable memory than is reserved by query");

        Long taskReservation = taskRevocableMemoryReservations.get(taskId);
        requireNonNull(taskReservation, "taskReservation is null");
        checkArgument(taskReservation >= bytes, "tried to free more revocable memory than is reserved by task");

        queryReservation -= bytes;
        if (queryReservation == 0) {
            queryRevocableMemoryReservations.remove(queryId);
        }
        else {
            queryRevocableMemoryReservations.put(queryId, queryReservation);
        }

        taskReservation -= bytes;
        if (taskReservation == 0) {
            taskRevocableMemoryReservations.remove(taskId);
        }
        else {
            taskRevocableMemoryReservations.put(taskId, taskReservation);
        }

        reservedRevocableBytes -= bytes;
        if (getFreeBytes() > 0 && future != null) {
            future.set(null);
            future = null;
        }
    }

    public synchronized void freeRevocable(long bytes)
    {
        checkArgument(bytes >= 0, "'%s' is negative", bytes);
        checkArgument(reservedRevocableBytes >= bytes, "tried to free more revocable memory than is reserved");
        if (bytes == 0) {
            // Freeing zero bytes is a no-op
            return;
        }

        reservedRevocableBytes -= bytes;
        if (getFreeBytes() > 0 && future != null) {
            future.set(null);
            future = null;
        }
    }

    /**
     * Returns the number of free bytes. This value may be negative, which indicates that the pool is over-committed.
     */
    @Managed
    public synchronized long getFreeBytes()
    {
        return maxBytes - reservedBytes - reservedRevocableBytes;
    }

    @Managed
    public long getMaxBytes()
    {
        return maxBytes;
    }

    @Managed
    public synchronized long getReservedBytes()
    {
        return reservedBytes;
    }

    @Managed
    public synchronized long getReservedRevocableBytes()
    {
        return reservedRevocableBytes;
    }

    synchronized long getQueryMemoryReservation(QueryId queryId)
    {
        return queryMemoryReservations.getOrDefault(queryId, 0L);
    }

    @VisibleForTesting
    synchronized long getQueryRevocableMemoryReservation(QueryId queryId)
    {
        return queryRevocableMemoryReservations.getOrDefault(queryId, 0L);
    }

    @VisibleForTesting
    synchronized long getTaskMemoryReservation(TaskId taskId)
    {
        return taskMemoryReservations.getOrDefault(taskId, 0L);
    }

    @VisibleForTesting
    synchronized long getTaskRevocableMemoryReservation(TaskId taskId)
    {
        return taskRevocableMemoryReservations.getOrDefault(taskId, 0L);
    }

    @Override
    public synchronized String toString()
    {
        return toStringHelper(this)
                .add("maxBytes", maxBytes)
                .add("freeBytes", getFreeBytes())
                .add("reservedBytes", reservedBytes)
                .add("reservedRevocableBytes", reservedRevocableBytes)
                .add("future", future)
                .toString();
    }

    private static class NonCancellableMemoryFuture<V>
            extends AbstractFuture<V>
    {
        public static <V> NonCancellableMemoryFuture<V> create()
        {
            return new NonCancellableMemoryFuture<>();
        }

        @Override
        public boolean set(@Nullable V value)
        {
            return super.set(value);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning)
        {
            throw new UnsupportedOperationException("cancellation is not supported");
        }
    }

    private synchronized void updateTaggedMemoryAllocations(QueryId queryId, String allocationTag, long delta)
    {
        if (delta == 0) {
            return;
        }

        Map<String, Long> allocations = taggedMemoryAllocations.computeIfAbsent(queryId, ignored -> new HashMap<>());
        allocations.compute(allocationTag, (ignored, oldValue) -> {
            if (oldValue == null) {
                return delta;
            }
            long newValue = oldValue.longValue() + delta;
            if (newValue == 0) {
                return null;
            }
            return newValue;
        });
    }

    @VisibleForTesting
    public synchronized Map<QueryId, Long> getQueryMemoryReservations()
    {
        return ImmutableMap.copyOf(queryMemoryReservations);
    }

    @VisibleForTesting
    public synchronized Map<QueryId, Map<String, Long>> getTaggedMemoryAllocations()
    {
        return ImmutableMap.copyOf(taggedMemoryAllocations);
    }

    @VisibleForTesting
    public synchronized Map<QueryId, Long> getQueryRevocableMemoryReservations()
    {
        return ImmutableMap.copyOf(queryRevocableMemoryReservations);
    }

    @VisibleForTesting
    public synchronized Map<TaskId, Long> getTaskMemoryReservations()
    {
        return ImmutableMap.copyOf(taskMemoryReservations);
    }

    @VisibleForTesting
    public synchronized Map<TaskId, Long> getTaskRevocableMemoryReservations()
    {
        return ImmutableMap.copyOf(taskRevocableMemoryReservations);
    }
}
