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
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import com.google.common.math.Stats;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.trino.annotation.NotThreadSafe;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.StageId;
import io.trino.metadata.Split;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.sql.planner.plan.PlanNodeId;
import org.weakref.jmx.Managed;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.ImmutableSetMultimap.toImmutableSetMultimap;
import static io.airlift.units.DataSize.succinctBytes;
import static io.trino.spi.StandardErrorCode.EXCEEDED_TASK_DESCRIPTOR_STORAGE_CAPACITY;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TaskDescriptorStorage
{
    private static final Logger log = Logger.get(TaskDescriptorStorage.class);

    private final long maxMemoryInBytes;
    private final JsonCodec<Split> splitJsonCodec;

    @GuardedBy("this")
    private final Map<QueryId, TaskDescriptors> storages = new HashMap<>();
    @GuardedBy("this")
    private long reservedBytes;

    @Inject
    public TaskDescriptorStorage(
            QueryManagerConfig config,
            JsonCodec<Split> splitJsonCodec)
    {
        this(config.getFaultTolerantExecutionTaskDescriptorStorageMaxMemory(), splitJsonCodec);
    }

    public TaskDescriptorStorage(DataSize maxMemory, JsonCodec<Split> splitJsonCodec)
    {
        this.maxMemoryInBytes = maxMemory.toBytes();
        this.splitJsonCodec = requireNonNull(splitJsonCodec, "splitJsonCodec is null");
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
     * Removes {@link TaskDescriptor} for a task identified by the <code>stageId</code> and <code>partitionId</code>.
     * If the query has been terminated the call is ignored.
     *
     * @throws java.util.NoSuchElementException if {@link TaskDescriptor} for a given task does not exist
     */
    public synchronized void remove(StageId stageId, int partitionId)
    {
        TaskDescriptors storage = storages.get(stageId.getQueryId());
        if (storage == null) {
            // query has been terminated
            return;
        }
        long previousReservedBytes = storage.getReservedBytes();
        storage.remove(stageId, partitionId);
        long currentReservedBytes = storage.getReservedBytes();
        long delta = currentReservedBytes - previousReservedBytes;
        updateMemoryReservation(delta);
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

            if (log.isInfoEnabled()) {
                log.info("Failing query %s; reclaiming %s of %s task descriptor memory from %s queries; extraStorageInfo=%s", killCandidate, storage.getReservedBytes(), succinctBytes(reservedBytes), storages.size(), storage.getDebugInfo());
            }

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
    private class TaskDescriptors
    {
        private final Table<StageId, Integer /* partitionId */, TaskDescriptor> descriptors = HashBasedTable.create();

        private long reservedBytes;
        private RuntimeException failure;

        public void put(StageId stageId, int partitionId, TaskDescriptor descriptor)
        {
            throwIfFailed();
            checkState(!descriptors.contains(stageId, partitionId), "task descriptor is already present for key %s/%s ", stageId, partitionId);
            descriptors.put(stageId, partitionId, descriptor);
            reservedBytes += descriptor.getRetainedSizeInBytes();
        }

        public TaskDescriptor get(StageId stageId, int partitionId)
        {
            throwIfFailed();
            TaskDescriptor descriptor = descriptors.get(stageId, partitionId);
            if (descriptor == null) {
                throw new NoSuchElementException(format("descriptor not found for key %s/%s", stageId, partitionId));
            }
            return descriptor;
        }

        public void remove(StageId stageId, int partitionId)
        {
            throwIfFailed();
            TaskDescriptor descriptor = descriptors.remove(stageId, partitionId);
            if (descriptor == null) {
                throw new NoSuchElementException(format("descriptor not found for key %s/%s", stageId, partitionId));
            }
            reservedBytes -= descriptor.getRetainedSizeInBytes();
        }

        public long getReservedBytes()
        {
            return reservedBytes;
        }

        private String getDebugInfo()
        {
            Multimap<StageId, TaskDescriptor> descriptorsByStageId = descriptors.cellSet().stream()
                    .collect(toImmutableSetMultimap(
                            Table.Cell::getRowKey,
                            Table.Cell::getValue));

            Map<StageId, String> debugInfoByStageId = descriptorsByStageId.asMap().entrySet().stream()
                    .collect(toImmutableMap(
                            Map.Entry::getKey,
                            entry -> getDebugInfo(entry.getValue())));

            List<String> biggestSplits = descriptorsByStageId.entries().stream()
                    .flatMap(entry -> entry.getValue().getSplits().entries().stream().map(splitEntry -> Map.entry("%s/%s".formatted(entry.getKey(), splitEntry.getKey()), splitEntry.getValue())))
                    .sorted(Comparator.<Map.Entry<String, Split>>comparingLong(entry -> entry.getValue().getRetainedSizeInBytes()).reversed())
                    .limit(3)
                    .map(entry -> "{nodeId=%s, size=%s, split=%s}".formatted(entry.getKey(), entry.getValue().getRetainedSizeInBytes(), splitJsonCodec.toJson(entry.getValue())))
                    .toList();

            return "stagesInfo=%s; biggestSplits=%s".formatted(debugInfoByStageId, biggestSplits);
        }

        private String getDebugInfo(Collection<TaskDescriptor> taskDescriptors)
        {
            int taskDescriptorsCount = taskDescriptors.size();
            Stats taskDescriptorsRetainedSizeStats = Stats.of(taskDescriptors.stream().mapToLong(TaskDescriptor::getRetainedSizeInBytes));

            Set<PlanNodeId> planNodeIds = taskDescriptors.stream().flatMap(taskDescriptor -> taskDescriptor.getSplits().keySet().stream()).collect(toImmutableSet());
            Map<PlanNodeId, String> splitsDebugInfo = new HashMap<>();
            for (PlanNodeId planNodeId : planNodeIds) {
                Stats splitCountStats = Stats.of(taskDescriptors.stream().mapToLong(taskDescriptor -> taskDescriptor.getSplits().asMap().get(planNodeId).size()));
                Stats splitSizeStats = Stats.of(taskDescriptors.stream().flatMap(taskDescriptor -> taskDescriptor.getSplits().get(planNodeId).stream()).mapToLong(Split::getRetainedSizeInBytes));
                splitsDebugInfo.put(
                        planNodeId,
                        "{splitCountMean=%s, splitCountStdDev=%s, splitSizeMean=%s, splitSizeStdDev=%s}".formatted(
                                splitCountStats.mean(),
                                splitCountStats.populationStandardDeviation(),
                                splitSizeStats.mean(),
                                splitSizeStats.populationStandardDeviation()));
            }

            return "[taskDescriptorsCount=%s, taskDescriptorsRetainedSizeMean=%s, taskDescriptorsRetainedSizeStdDev=%s, splits=%s]".formatted(
                    taskDescriptorsCount,
                    taskDescriptorsRetainedSizeStats.mean(),
                    taskDescriptorsRetainedSizeStats.populationStandardDeviation(),
                    splitsDebugInfo);
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
}
