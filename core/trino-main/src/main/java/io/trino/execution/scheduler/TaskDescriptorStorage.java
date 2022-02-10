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
package io.trino.execution.scheduler;

import com.google.common.base.VerifyException;
import io.airlift.units.DataSize;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.StageId;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import org.weakref.jmx.Managed;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.units.DataSize.succinctBytes;
import static io.trino.spi.StandardErrorCode.EXCEEDED_TASK_DESCRIPTOR_STORAGE_CAPACITY;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TaskDescriptorStorage
{
    private final long maxMemoryInBytes;

    @GuardedBy("this")
    private final Map<QueryId, TaskDescriptors> storages = new HashMap<>();
    @GuardedBy("this")
    private long reservedBytes;

    @Inject
    public TaskDescriptorStorage(QueryManagerConfig config)
    {
        this(requireNonNull(config, "config is null").getFaultTolerantExecutionTaskDescriptorStorageMaxMemory());
    }

    public TaskDescriptorStorage(DataSize maxMemory)
    {
        this.maxMemoryInBytes = requireNonNull(maxMemory, "maxMemory is null").toBytes();
    }

    /**
     * Initializes task descriptor storage for a given <code>queryId</code>.
     * It is expected to be called before query scheduling begins.
     */
    public synchronized void initialize(QueryId queryId)
    {
        TaskDescriptors storage = new TaskDescriptors();
        verify(storages.putIfAbsent(queryId, storage) == null, "storage is already initialized for query: %s", queryId);
        updateMemoryReservation(storage.getReservedBytes());
    }

    /**
     * Stores {@link TaskDescriptor} for a task identified by the <code>stageId</code> and <code>partitionId</code>.
     * The <code>partitionId</code> is obtained from the {@link TaskDescriptor} by calling {@link TaskDescriptor#getPartitionId()}.
     * If the query has been terminated the call is ignored.
     *
     * @throws IllegalStateException if the storage already has a task descriptor for a given task
     */
    public synchronized void put(StageId stageId, TaskDescriptor descriptor)
    {
        TaskDescriptors storage = storages.get(stageId.getQueryId());
        if (storage == null) {
            // query has been terminated
            return;
        }
        long previousReservedBytes = storage.getReservedBytes();
        storage.put(stageId, descriptor.getPartitionId(), descriptor);
        long currentReservedBytes = storage.getReservedBytes();
        long delta = currentReservedBytes - previousReservedBytes;
        updateMemoryReservation(delta);
    }

    /**
     * Get task descriptor
     *
     * @return Non empty {@link TaskDescriptor} for a task identified by the <code>stageId</code> and <code>partitionId</code>.
     * Returns {@link Optional#empty()} if the query of a given <code>stageId</code> has been finished (e.g.: cancelled by the user or finished early).
     * @throws java.util.NoSuchElementException if {@link TaskDescriptor} for a given task does not exist
     */
    public synchronized Optional<TaskDescriptor> get(StageId stageId, int partitionId)
    {
        TaskDescriptors storage = storages.get(stageId.getQueryId());
        if (storage == null) {
            // query has been terminated
            return Optional.empty();
        }
        return Optional.of(storage.get(stageId, partitionId));
    }

    /**
     * Notifies the storage that the query with a given <code>queryId</code> has been finished and the task descriptors can be safely discarded.
     * <p>
     * The engine may decided to destroy the storage while the scheduling is still in process (for example if query was cancelled). Under such
     * circumstances the implementation will ignore future calls to {@link #put(StageId, TaskDescriptor)} and return
     * {@link Optional#empty()} from {@link #get(StageId, int)}. The scheduler is expected to handle this condition appropriately.
     */
    public synchronized void destroy(QueryId queryId)
    {
        TaskDescriptors storage = storages.remove(queryId);
        if (storage != null) {
            updateMemoryReservation(-storage.getReservedBytes());
        }
    }

    private synchronized void updateMemoryReservation(long delta)
    {
        reservedBytes += delta;
        if (delta <= 0) {
            return;
        }
        while (reservedBytes > maxMemoryInBytes) {
            // drop a query that uses the most storage
            QueryId killCandidate = storages.entrySet().stream()
                    .max(Comparator.comparingLong(entry -> entry.getValue().getReservedBytes()))
                    .map(Map.Entry::getKey)
                    .orElseThrow(() -> new VerifyException(format("storage is empty but reservedBytes (%s) is still greater than maxMemoryInBytes (%s)", reservedBytes, maxMemoryInBytes)));
            TaskDescriptors storage = storages.get(killCandidate);
            long previousReservedBytes = storage.getReservedBytes();
            storage.fail(new TrinoException(
                    EXCEEDED_TASK_DESCRIPTOR_STORAGE_CAPACITY,
                    format("Task descriptor storage capacity has been exceeded: %s > %s", succinctBytes(maxMemoryInBytes), succinctBytes(reservedBytes))));
            long currentReservedBytes = storage.getReservedBytes();
            reservedBytes += (currentReservedBytes - previousReservedBytes);
        }
    }

    @Managed
    public synchronized long getReservedBytes()
    {
        return reservedBytes;
    }

    @NotThreadSafe
    private static class TaskDescriptors
    {
        private final Map<TaskDescriptorKey, TaskDescriptor> descriptors = new HashMap<>();
        private long reservedBytes;
        private RuntimeException failure;

        public void put(StageId stageId, int partitionId, TaskDescriptor descriptor)
        {
            throwIfFailed();
            TaskDescriptorKey key = new TaskDescriptorKey(stageId, partitionId);
            checkState(descriptors.putIfAbsent(key, descriptor) == null, "task descriptor is already present for key %s ", key);
            reservedBytes += descriptor.getRetainedSizeInBytes();
        }

        public TaskDescriptor get(StageId stageId, int partitionId)
        {
            throwIfFailed();
            TaskDescriptorKey key = new TaskDescriptorKey(stageId, partitionId);
            TaskDescriptor descriptor = descriptors.get(key);
            if (descriptor == null) {
                throw new NoSuchElementException(format("descriptor not found for key %s", key));
            }
            return descriptor;
        }

        public long getReservedBytes()
        {
            return reservedBytes;
        }

        private void fail(RuntimeException failure)
        {
            if (this.failure == null) {
                descriptors.clear();
                reservedBytes = 0;
                this.failure = failure;
            }
        }

        private void throwIfFailed()
        {
            if (failure != null) {
                throw failure;
            }
        }
    }

    private static class TaskDescriptorKey
    {
        private final StageId stageId;
        private final int partitionId;

        private TaskDescriptorKey(StageId stageId, int partitionId)
        {
            this.stageId = requireNonNull(stageId, "stageId is null");
            this.partitionId = partitionId;
        }

        public StageId getStageId()
        {
            return stageId;
        }

        public int getPartitionId()
        {
            return partitionId;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TaskDescriptorKey key = (TaskDescriptorKey) o;
            return partitionId == key.partitionId && Objects.equals(stageId, key.stageId);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(stageId, partitionId);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("stageId", stageId)
                    .add("partitionId", partitionId)
                    .toString();
        }
    }
}
