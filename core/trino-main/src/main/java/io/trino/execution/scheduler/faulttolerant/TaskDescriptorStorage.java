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
package io.trino.execution.scheduler.faulttolerant;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.base.VerifyException;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import com.google.common.math.Quantiles;
import com.google.common.math.Stats;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.compress.v3.zstd.ZstdCompressor;
import io.airlift.compress.v3.zstd.ZstdDecompressor;
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
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.ImmutableSetMultimap.toImmutableSetMultimap;
import static com.google.common.math.Quantiles.percentiles;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.units.DataSize.succinctBytes;
import static io.trino.spi.StandardErrorCode.EXCEEDED_TASK_DESCRIPTOR_STORAGE_CAPACITY;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.lang.System.arraycopy;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class TaskDescriptorStorage
{
    private static final Logger log = Logger.get(TaskDescriptorStorage.class);
    public static final int SINGLE_STEP_COMPRESSION_LIMIT = 1000;

    private final long maxMemoryInBytes;
    private final long compressingHighWaterMark;
    private final long compressingLowWaterMark;

    private final JsonCodec<TaskDescriptor> taskDescriptorJsonCodec;
    private final JsonCodec<Split> splitJsonCodec;
    private final StorageStats storageStats;

    @GuardedBy("this")
    private final Map<QueryId, TaskDescriptors> storages = new HashMap<>();

    /*
     * The following fields are used to track memory usage of the storage.
     * The memory usage is tracked in three categories:
     * 1. reservedUncompressedBytes: memory used by uncompressed task descriptors
     * 2. reservedCompressedBytes: memory used by compressed task descriptors
     * 3. originalCompressedBytes: how much memory was used by compressed task descriptors before compression (informational only)
     */
    @GuardedBy("this")
    private long reservedUncompressedBytes;
    @GuardedBy("this")
    private long reservedCompressedBytes;
    @GuardedBy("this")
    private long originalCompressedBytes;

    @GuardedBy("this")
    private boolean compressing;
    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor(daemonThreadsNamed("task-descriptor-storage"));
    private volatile boolean running;

    @Inject
    public TaskDescriptorStorage(
            QueryManagerConfig config,
            JsonCodec<TaskDescriptor> taskDescriptorJsonCodec,
            JsonCodec<Split> splitJsonCodec)
    {
        this(
                config.getFaultTolerantExecutionTaskDescriptorStorageMaxMemory(),
                config.getFaultTolerantExecutionTaskDescriptorStorageHighWaterMark(),
                config.getFaultTolerantExecutionTaskDescriptorStorageLowWaterMark(),
                taskDescriptorJsonCodec,
                splitJsonCodec);
    }

    public TaskDescriptorStorage(
            DataSize maxMemory,
            DataSize compressingHighWaterMark,
            DataSize compressingLowWaterMark,
            JsonCodec<TaskDescriptor> taskDescriptorJsonCodec,
            JsonCodec<Split> splitJsonCodec)
    {
        this.maxMemoryInBytes = maxMemory.toBytes();
        this.compressingHighWaterMark = compressingHighWaterMark.toBytes();
        this.compressingLowWaterMark = compressingLowWaterMark.toBytes();
        this.taskDescriptorJsonCodec = requireNonNull(taskDescriptorJsonCodec, "taskDescriptorJsonCodec is null");
        this.splitJsonCodec = requireNonNull(splitJsonCodec, "splitJsonCodec is null");
        this.storageStats = new StorageStats(Suppliers.memoizeWithExpiration(this::computeStats, 1, TimeUnit.SECONDS));
    }

    @PostConstruct
    public void start()
    {
        running = true;
        executor.schedule(this::compressTaskDescriptorsJob, 10, TimeUnit.SECONDS);
    }

    private void compressTaskDescriptorsJob()
    {
        if (!running) {
            return;
        }
        int delaySeconds = 10;
        try {
            if (!compressTaskDescriptorsStep()) {
                delaySeconds = 0;
            }
        }
        catch (Throwable e) {
            log.error(e, "Error in compressTaskDescriptorsJob");
        }
        finally {
            executor.schedule(this::compressTaskDescriptorsJob, delaySeconds, TimeUnit.SECONDS);
        }
    }

    @PreDestroy
    public void stop()
    {
        running = false;
        executor.shutdownNow();
    }

    private boolean compressTaskDescriptorsStep()
    {
        int limit = SINGLE_STEP_COMPRESSION_LIMIT;
        synchronized (this) {
            if (!compressing) {
                return true;
            }

            for (Map.Entry<QueryId, TaskDescriptors> entry : storages.entrySet()) {
                if (limit <= 0) {
                    return false;
                }
                TaskDescriptors storage = entry.getValue();
                if (!storage.isFullyCompressed()) {
                    var holder = new Object()
                    {
                        int limitDelta;
                    };
                    int limitFinal = limit;
                    runAndUpdateMemory(storage, () -> holder.limitDelta = storage.compress(limitFinal), false);
                    limit -= holder.limitDelta;
                }
            }
        }
        return limit > 0;
    }

    /**
     * Initializes task descriptor storage for a given <code>queryId</code>.
     * It is expected to be called before query scheduling begins.
     */
    public synchronized void initialize(QueryId queryId)
    {
        TaskDescriptors storage = new TaskDescriptors();
        verify(storages.putIfAbsent(queryId, storage) == null, "storage is already initialized for query: %s", queryId);
        updateMemoryReservation(storage.getReservedUncompressedBytes(), storage.getReservedCompressedBytes(), storage.getOriginalCompressedBytes(), true);
        updateCompressingFlag();
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

        runAndUpdateMemory(storage, () -> storage.put(stageId, descriptor.getPartitionId(), descriptor), true);
    }

    @GuardedBy("this")
    private void runAndUpdateMemory(TaskDescriptors storage, Runnable operation, boolean considerKilling)
    {
        long previousReservedUncompressedBytes = storage.getReservedUncompressedBytes();
        long previousReservedCompressedBytes = storage.getReservedCompressedBytes();
        long previousOriginalCompressedBytes = storage.getOriginalCompressedBytes();

        operation.run();

        long currentReservedUncompressedBytes = storage.getReservedUncompressedBytes();
        long currentReservedCompressedBytes = storage.getReservedCompressedBytes();
        long currentOriginalCompressedBytes = storage.getOriginalCompressedBytes();

        long reservedUncompressedDelta = currentReservedUncompressedBytes - previousReservedUncompressedBytes;
        long reservedCompressedDelta = currentReservedCompressedBytes - previousReservedCompressedBytes;
        long originalCompressedDelta = currentOriginalCompressedBytes - previousOriginalCompressedBytes;

        updateMemoryReservation(reservedUncompressedDelta, reservedCompressedDelta, originalCompressedDelta, considerKilling);
        updateCompressingFlag();
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

        runAndUpdateMemory(storage, () -> storage.remove(stageId, partitionId), false);
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
            updateMemoryReservation(-storage.getReservedUncompressedBytes(), -storage.getReservedCompressedBytes(), -storage.getOriginalCompressedBytes(), false);
            updateCompressingFlag();
        }
    }

    @GuardedBy("this")
    private void updateCompressingFlag()
    {
        if (!compressing && originalCompressedBytes + reservedUncompressedBytes > compressingHighWaterMark) {
            compressing = true;
        }
        else if (compressing && originalCompressedBytes + reservedUncompressedBytes < compressingLowWaterMark) {
            compressing = false;
        }
    }

    @GuardedBy("this")
    private void updateMemoryReservation(long reservedUncompressedDelta, long reservedCompressedDelta, long originalCompressedDelta, boolean considerKilling)
    {
        reservedUncompressedBytes += reservedUncompressedDelta;
        reservedCompressedBytes += reservedCompressedDelta;
        originalCompressedBytes += originalCompressedDelta;
        checkStatsNotNegative();

        if (reservedUncompressedDelta + reservedCompressedDelta <= 0 || !considerKilling) {
            return;
        }

        while (reservedUncompressedBytes + reservedCompressedBytes > maxMemoryInBytes) {
            // drop a query that uses the most storage
            QueryId killCandidate = storages.entrySet().stream()
                    .max(Comparator.comparingLong(entry -> entry.getValue().getReservedBytes()))
                    .map(Map.Entry::getKey)
                    .orElseThrow(() -> new VerifyException(format("storage is empty but reservedBytes (%s + %s) is still greater than maxMemoryInBytes (%s)", reservedUncompressedDelta, reservedCompressedDelta, maxMemoryInBytes)));
            TaskDescriptors storage = storages.get(killCandidate);

            if (log.isInfoEnabled()) {
                log.info("Failing query %s; reclaiming %s of %s/%s task descriptor memory from %s queries; extraStorageInfo=%s", killCandidate, storage.getReservedBytes(), succinctBytes(reservedUncompressedBytes), succinctBytes(reservedCompressedBytes), storages.size(), storage.getDebugInfo());
            }

            runAndUpdateMemory(
                    storage,
                    () -> storage.fail(new TrinoException(
                            EXCEEDED_TASK_DESCRIPTOR_STORAGE_CAPACITY,
                            format("Task descriptor storage capacity has been exceeded: %s > %s", succinctBytes(reservedUncompressedBytes + reservedCompressedBytes), succinctBytes(maxMemoryInBytes)))),
                    false);
        }
    }

    @GuardedBy("this")
    private void checkStatsNotNegative()
    {
        checkState(reservedUncompressedBytes >= 0, "reservedUncompressedBytes is negative");
        checkState(reservedUncompressedBytes >= 0, "reservedCompressedBytes is negative");
        checkState(originalCompressedBytes >= 0, "originalCompressedBytes is negative");
    }

    @VisibleForTesting
    synchronized long getReservedUncompressedBytes()
    {
        return reservedUncompressedBytes;
    }

    @VisibleForTesting
    synchronized long getReservedCompressedBytes()
    {
        return reservedCompressedBytes;
    }

    @VisibleForTesting
    synchronized long getOriginalCompressedBytes()
    {
        return originalCompressedBytes;
    }

    private TaskDescriptorHolder createTaskDescriptorHolder(TaskDescriptor taskDescriptor)
    {
        return new TaskDescriptorHolder(taskDescriptor);
    }

    @Managed
    @Nested
    public StorageStats getStats()
    {
        // This should not contain materialized values. GuiceMBeanExporter calls it only once during application startup
        // and then only @Managed methods all called on that instance.
        return storageStats;
    }

    private synchronized StorageStatsValues computeStats()
    {
        int queriesCount = storages.size();
        long stagesCount = storages.values().stream().mapToLong(TaskDescriptors::getStagesCount).sum();

        StorageStatsValue uncompressedReservedStats = getStorageStatsValue(
                queriesCount,
                stagesCount,
                reservedUncompressedBytes,
                TaskDescriptors::getReservedUncompressedBytes,
                TaskDescriptors::getStagesReservedUncompressedBytes);
        StorageStatsValue compressedReservedStats = getStorageStatsValue(
                queriesCount,
                stagesCount,
                reservedCompressedBytes,
                TaskDescriptors::getReservedCompressedBytes,
                TaskDescriptors::getStagesReservedCompressedBytes);
        StorageStatsValue originalCompressedStats = getStorageStatsValue(
                queriesCount,
                stagesCount,
                originalCompressedBytes,
                TaskDescriptors::getOriginalCompressedBytes,
                TaskDescriptors::getStagesOriginalCompressedBytes);
        return new StorageStatsValues(
                queriesCount,
                stagesCount,
                compressing,
                uncompressedReservedStats,
                compressedReservedStats,
                originalCompressedStats);
    }

    @GuardedBy("this")
    private StorageStatsValue getStorageStatsValue(
            int queriesCount,
            long stagesCount,
            long totalBytes,
            Function<TaskDescriptors, Long> queryBytes,
            Function<TaskDescriptors, Stream<? extends Long>> stageBytes)
    {
        Quantiles.ScaleAndIndexes percentiles = percentiles().indexes(50, 90, 95);

        long queryBytesP50 = 0;
        long queryBytesP90 = 0;
        long queryBytesP95 = 0;
        long queryBytesAvg = 0;
        long stageBytesP50 = 0;
        long stageBytesP90 = 0;
        long stageBytesP95 = 0;
        long stageBytesAvg = 0;

        if (queriesCount > 0) { // we cannot compute percentiles for empty set

            Map<Integer, Double> queryBytesPercentiles = percentiles.compute(
                    storages.values().stream()
                            .map(queryBytes)
                            .collect(toImmutableList()));

            queryBytesP50 = queryBytesPercentiles.get(50).longValue();
            queryBytesP90 = queryBytesPercentiles.get(90).longValue();
            queryBytesP95 = queryBytesPercentiles.get(95).longValue();
            queryBytesAvg = totalBytes / queriesCount;

            List<Long> storagesReservedBytes = storages.values().stream()
                    .flatMap(stageBytes)
                    .collect(toImmutableList());

            if (!storagesReservedBytes.isEmpty()) {
                Map<Integer, Double> stagesReservedBytesPercentiles = percentiles.compute(
                        storagesReservedBytes);
                stageBytesP50 = stagesReservedBytesPercentiles.get(50).longValue();
                stageBytesP90 = stagesReservedBytesPercentiles.get(90).longValue();
                stageBytesP95 = stagesReservedBytesPercentiles.get(95).longValue();
                stageBytesAvg = totalBytes / stagesCount;
            }
        }

        return new StorageStatsValue(
                totalBytes,
                queryBytesAvg,
                queryBytesP50,
                queryBytesP90,
                queryBytesP95,
                stageBytesAvg,
                stageBytesP50,
                stageBytesP90,
                stageBytesP95);
    }

    @NotThreadSafe
    private class TaskDescriptors
    {
        private final Table<StageId, Integer /* partitionId */, TaskDescriptorHolder> descriptors = HashBasedTable.create();
        public boolean fullyCompressed;

        /* tracking of memory usage; see top level comment in TaskDescriptorStorage for meaning */
        private long reservedUncompressedBytes;
        private long reservedCompressedBytes;
        private long originalCompressedBytes;

        private final Map<StageId, AtomicLong> stagesReservedUncompressedBytes = new HashMap<>();
        private final Map<StageId, AtomicLong> stagesReservedCompressedBytes = new HashMap<>();
        private final Map<StageId, AtomicLong> stagesOriginalCompressedBytes = new HashMap<>();
        private TrinoException failure;

        @GuardedBy("TaskDescriptorStorage.this")
        public void put(StageId stageId, int partitionId, TaskDescriptor descriptor)
        {
            throwIfFailed();
            checkState(!descriptors.contains(stageId, partitionId), "task descriptor is already present for key %s/%s ", stageId, partitionId);
            TaskDescriptorHolder descriptorHolder = createTaskDescriptorHolder(descriptor);

            if (compressing) {
                descriptorHolder.compress();
            }
            else {
                fullyCompressed = false;
            }
            descriptors.put(stageId, partitionId, descriptorHolder);

            if (descriptorHolder.isCompressed()) {
                originalCompressedBytes += descriptorHolder.getUncompressedSize();
                reservedCompressedBytes += descriptorHolder.getCompressedSize();
                stagesOriginalCompressedBytes.computeIfAbsent(stageId, _ -> new AtomicLong()).addAndGet(descriptorHolder.getUncompressedSize());
                stagesReservedCompressedBytes.computeIfAbsent(stageId, _ -> new AtomicLong()).addAndGet(descriptorHolder.getCompressedSize());
            }
            else {
                reservedUncompressedBytes += descriptorHolder.getUncompressedSize();
                stagesReservedUncompressedBytes.computeIfAbsent(stageId, _ -> new AtomicLong()).addAndGet(descriptorHolder.getUncompressedSize());
            }
        }

        public TaskDescriptor get(StageId stageId, int partitionId)
        {
            throwIfFailed();
            TaskDescriptorHolder descriptor = descriptors.get(stageId, partitionId);
            if (descriptor == null) {
                throw new NoSuchElementException(format("descriptor not found for key %s/%s", stageId, partitionId));
            }
            return descriptor.getTaskDescriptor();
        }

        public void remove(StageId stageId, int partitionId)
        {
            throwIfFailed();
            TaskDescriptorHolder descriptorHolder = descriptors.remove(stageId, partitionId);
            if (descriptorHolder == null) {
                throw new NoSuchElementException(format("descriptor not found for key %s/%s", stageId, partitionId));
            }

            if (descriptorHolder.isCompressed()) {
                originalCompressedBytes -= descriptorHolder.getUncompressedSize();
                reservedCompressedBytes -= descriptorHolder.getCompressedSize();
                stagesOriginalCompressedBytes.computeIfAbsent(stageId, _ -> new AtomicLong()).addAndGet(-descriptorHolder.getUncompressedSize());
                stagesReservedCompressedBytes.computeIfAbsent(stageId, _ -> new AtomicLong()).addAndGet(-descriptorHolder.getCompressedSize());
            }
            else {
                reservedUncompressedBytes -= descriptorHolder.getUncompressedSize();
                stagesReservedUncompressedBytes.computeIfAbsent(stageId, _ -> new AtomicLong()).addAndGet(-descriptorHolder.getUncompressedSize());
            }
        }

        public long getReservedUncompressedBytes()
        {
            return reservedUncompressedBytes;
        }

        public long getReservedCompressedBytes()
        {
            return reservedCompressedBytes;
        }

        public long getOriginalCompressedBytes()
        {
            return originalCompressedBytes;
        }

        public long getReservedBytes()
        {
            return reservedUncompressedBytes + reservedCompressedBytes;
        }

        private String getDebugInfo()
        {
            Multimap<StageId, TaskDescriptorHolder> descriptorsByStageId = descriptors.cellSet().stream()
                    .collect(toImmutableSetMultimap(
                            Table.Cell::getRowKey,
                            Table.Cell::getValue));

            Map<StageId, String> debugInfoByStageId = descriptorsByStageId.asMap().entrySet().stream()
                    .collect(toImmutableMap(
                            Map.Entry::getKey,
                            entry -> getDebugInfo(entry.getValue())));

            List<String> biggestSplits = descriptorsByStageId.entries().stream()
                    .flatMap(entry -> entry.getValue().getTaskDescriptor().getSplits().getSplitsFlat().entries().stream().map(splitEntry -> Map.entry("%s/%s".formatted(entry.getKey(), splitEntry.getKey()), splitEntry.getValue())))
                    .sorted(Comparator.<Map.Entry<String, Split>>comparingLong(entry -> entry.getValue().getRetainedSizeInBytes()).reversed())
                    .limit(3)
                    .map(entry -> "{nodeId=%s, size=%s, split=%s}".formatted(entry.getKey(), entry.getValue().getRetainedSizeInBytes(), splitJsonCodec.toJson(entry.getValue())))
                    .toList();

            return "stagesInfo=%s; biggestSplits=%s".formatted(debugInfoByStageId, biggestSplits);
        }

        private String getDebugInfo(Collection<TaskDescriptorHolder> taskDescriptors)
        {
            int taskDescriptorsCount = taskDescriptors.size();
            Stats taskDescriptorsRetainedSizeStats = Stats.of(taskDescriptors.stream().mapToLong(TaskDescriptorHolder::getRetainedSizeInBytes));

            Set<PlanNodeId> planNodeIds = taskDescriptors.stream().flatMap(taskDescriptor -> taskDescriptor.getTaskDescriptor().getSplits().getSplitsFlat().keySet().stream()).collect(toImmutableSet());
            Map<PlanNodeId, String> splitsDebugInfo = new HashMap<>();
            for (PlanNodeId planNodeId : planNodeIds) {
                Stats splitCountStats = Stats.of(taskDescriptors.stream().mapToLong(taskDescriptor -> taskDescriptor.getTaskDescriptor().getSplits().getSplitsFlat().asMap().get(planNodeId).size()));
                Stats splitSizeStats = Stats.of(taskDescriptors.stream().flatMap(taskDescriptor -> taskDescriptor.getTaskDescriptor().getSplits().getSplitsFlat().get(planNodeId).stream()).mapToLong(Split::getRetainedSizeInBytes));
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

        private void fail(TrinoException failure)
        {
            if (this.failure == null) {
                descriptors.clear();
                reservedUncompressedBytes = 0;
                reservedCompressedBytes = 0;
                originalCompressedBytes = 0;
                this.failure = failure;
            }
        }

        private void throwIfFailed()
        {
            if (failure != null) {
                // add caller stack trace to the exception
                throw new TrinoException(failure::getErrorCode, failure.getMessage(), failure);
            }
        }

        public int getStagesCount()
        {
            return descriptors.rowMap().size();
        }

        public Stream<Long> getStagesReservedUncompressedBytes()
        {
            return stagesReservedUncompressedBytes.values().stream()
                    .map(AtomicLong::get);
        }

        public Stream<Long> getStagesReservedCompressedBytes()
        {
            return stagesReservedCompressedBytes.values().stream()
                    .map(AtomicLong::get);
        }

        public Stream<Long> getStagesOriginalCompressedBytes()
        {
            return stagesOriginalCompressedBytes.values().stream()
                    .map(AtomicLong::get);
        }

        public boolean isFullyCompressed()
        {
            return fullyCompressed;
        }

        @GuardedBy("TaskDescriptorStorage.this")
        public int compress(int limit)
        {
            if (fullyCompressed) {
                return 0;
            }
            List<TaskDescriptorHolder> selectedForCompresssion = descriptors.values().stream()
                    .filter(descriptor -> !descriptor.isCompressed())
                    .limit(limit)
                    .collect(toImmutableList());

            for (TaskDescriptorHolder holder : selectedForCompresssion) {
                long uncompressedSize = holder.getUncompressedSize();
                holder.compress();
                reservedUncompressedBytes -= uncompressedSize;
                originalCompressedBytes += uncompressedSize;
                reservedCompressedBytes += holder.getCompressedSize();
                checkStatsNotNegative();
            }
            if (selectedForCompresssion.size() < limit) {
                fullyCompressed = true;
            }
            return selectedForCompresssion.size();
        }
    }

    private record StorageStatsValues(
            long queriesCount,
            long stagesCount,
            boolean compressionActive,
            StorageStatsValue uncompressedReservedStats,
            StorageStatsValue compressedReservedStats,
            StorageStatsValue originalCompressedStats)
    {
        private StorageStatsValues
        {
            requireNonNull(uncompressedReservedStats, "uncompressedReservedStats is null");
            requireNonNull(compressedReservedStats, "compressedReservedStats is null");
            requireNonNull(originalCompressedStats, "originalCompressedStats is null");
        }
    }

    private record StorageStatsValue(
            long bytes,
            long queryBytesAvg,
            long queryBytesP50,
            long queryBytesP90,
            long queryBytesP95,
            long stageBytesAvg,
            long stageBytesP50,
            long stageBytesP90,
            long stageBytesP95) {}

    public static class StorageStats
    {
        private final Supplier<StorageStatsValues> statsSupplier;

        StorageStats(Supplier<StorageStatsValues> statsSupplier)
        {
            this.statsSupplier = requireNonNull(statsSupplier, "statsSupplier is null");
        }

        @Managed
        public long getQueriesCount()
        {
            return statsSupplier.get().queriesCount();
        }

        @Managed
        public long getStagesCount()
        {
            return statsSupplier.get().stagesCount();
        }

        @Managed
        public long getCompressionActive()
        {
            return statsSupplier.get().compressionActive() ? 1 : 0;
        }

        @Managed
        public long getUncompressedReservedBytes()
        {
            return statsSupplier.get().uncompressedReservedStats().bytes();
        }

        @Managed
        public long getQueryUncompressedReservedBytesAvg()
        {
            return statsSupplier.get().uncompressedReservedStats().queryBytesAvg();
        }

        @Managed
        public long getQueryUncompressedReservedBytesP50()
        {
            return statsSupplier.get().uncompressedReservedStats().queryBytesP50();
        }

        @Managed
        public long getQueryUncompressedReservedBytesP90()
        {
            return statsSupplier.get().uncompressedReservedStats().queryBytesP90();
        }

        @Managed
        public long getQueryUncompressedReservedBytesP95()
        {
            return statsSupplier.get().uncompressedReservedStats().queryBytesP95();
        }

        @Managed
        public long getStageUncompressedReservedBytesAvg()
        {
            return statsSupplier.get().uncompressedReservedStats().stageBytesP50();
        }

        @Managed
        public long getStageUncompressedReservedBytesP50()
        {
            return statsSupplier.get().uncompressedReservedStats().stageBytesP50();
        }

        @Managed
        public long getStageUncompressedReservedBytesP90()
        {
            return statsSupplier.get().uncompressedReservedStats().stageBytesP90();
        }

        @Managed
        public long getStageUncompressedReservedBytesP95()
        {
            return statsSupplier.get().uncompressedReservedStats().stageBytesP95();
        }

        @Managed
        public long getCompressedReservedBytes()
        {
            return statsSupplier.get().compressedReservedStats().bytes();
        }

        @Managed
        public long getQueryCompressedReservedBytesAvg()
        {
            return statsSupplier.get().compressedReservedStats().queryBytesAvg();
        }

        @Managed
        public long getQueryCompressedReservedBytesP50()
        {
            return statsSupplier.get().compressedReservedStats().queryBytesP50();
        }

        @Managed
        public long getQueryCompressedReservedBytesP90()
        {
            return statsSupplier.get().compressedReservedStats().queryBytesP90();
        }

        @Managed
        public long getQueryCompressedReservedBytesP95()
        {
            return statsSupplier.get().compressedReservedStats().queryBytesP95();
        }

        @Managed
        public long getStageCompressedReservedBytesAvg()
        {
            return statsSupplier.get().compressedReservedStats().stageBytesP50();
        }

        @Managed
        public long getStageCompressedReservedBytesP50()
        {
            return statsSupplier.get().compressedReservedStats().stageBytesP50();
        }

        @Managed
        public long getStageCompressedReservedBytesP90()
        {
            return statsSupplier.get().compressedReservedStats().stageBytesP90();
        }

        @Managed
        public long getStageCompressedReservedBytesP95()
        {
            return statsSupplier.get().compressedReservedStats().stageBytesP95();
        }

        @Managed
        public long getOriginalCompressedBytes()
        {
            return statsSupplier.get().originalCompressedStats().bytes();
        }

        @Managed
        public long getQueryOriginalCompressedBytesAvg()
        {
            return statsSupplier.get().originalCompressedStats().queryBytesAvg();
        }

        @Managed
        public long getQueryOriginalCompressedBytesP50()
        {
            return statsSupplier.get().originalCompressedStats().queryBytesP50();
        }

        @Managed
        public long getQueryOriginalCompressedBytesP90()
        {
            return statsSupplier.get().originalCompressedStats().queryBytesP90();
        }

        @Managed
        public long getQueryOriginalCompressedBytesP95()
        {
            return statsSupplier.get().originalCompressedStats().queryBytesP95();
        }

        @Managed
        public long getStageOriginalCompressedBytesAvg()
        {
            return statsSupplier.get().originalCompressedStats().stageBytesP50();
        }

        @Managed
        public long getStageOriginalCompressedBytesP50()
        {
            return statsSupplier.get().originalCompressedStats().stageBytesP50();
        }

        @Managed
        public long getStageOriginalCompressedBytesP90()
        {
            return statsSupplier.get().originalCompressedStats().stageBytesP90();
        }

        @Managed
        public long getStageOriginalCompressedBytesP95()
        {
            return statsSupplier.get().originalCompressedStats().stageBytesP95();
        }
    }

    private class TaskDescriptorHolder
    {
        private static final int INSTANCE_SIZE = instanceSize(TaskDescriptorHolder.class);

        private TaskDescriptor taskDescriptor;
        private final long uncompressedSize;
        private byte[] compressedTaskDescriptor;

        private TaskDescriptorHolder(TaskDescriptor taskDescriptor)
        {
            this.taskDescriptor = requireNonNull(taskDescriptor, "taskDescriptor is null");
            this.uncompressedSize = taskDescriptor.getRetainedSizeInBytes();
        }

        public TaskDescriptor getTaskDescriptor()
        {
            if (taskDescriptor != null) {
                return taskDescriptor;
            }
            // decompress if needed
            verify(compressedTaskDescriptor != null, "compressedTaskDescriptor is null");
            ZstdDecompressor decompressor = ZstdDecompressor.create();
            long decompressedSize = decompressor.getDecompressedSize(compressedTaskDescriptor, 0, compressedTaskDescriptor.length);
            byte[] output = new byte[toIntExact(decompressedSize)];
            decompressor.decompress(compressedTaskDescriptor, 0, compressedTaskDescriptor.length, output, 0, output.length);
            return taskDescriptorJsonCodec.fromJson(output);
        }

        public void compress()
        {
            checkState(!isCompressed(), "TaskDescriptor is compressed");

            byte[] taskDescriptorJson = taskDescriptorJsonCodec.toJsonBytes(taskDescriptor);
            ZstdCompressor compressor = ZstdCompressor.create();
            int maxCompressedSize = compressor.maxCompressedLength(taskDescriptorJson.length);
            byte[] tmpCompressedTaskDescriptor = new byte[maxCompressedSize];
            int compressedSize = compressor.compress(taskDescriptorJson, 0, taskDescriptorJson.length, tmpCompressedTaskDescriptor, 0, maxCompressedSize);
            this.compressedTaskDescriptor = new byte[compressedSize];
            arraycopy(tmpCompressedTaskDescriptor, 0, compressedTaskDescriptor, 0, compressedSize);
            taskDescriptor = null;
        }

        public void decompress()
        {
            checkState(isCompressed(), "TaskDescriptor is not compressed");
            this.taskDescriptor = getTaskDescriptor();
            this.compressedTaskDescriptor = null;
        }

        public boolean isCompressed()
        {
            return compressedTaskDescriptor != null;
        }

        public long getUncompressedSize()
        {
            return uncompressedSize;
        }

        public long getCompressedSize()
        {
            checkState(isCompressed(), "TaskDescriptor is not compressed");
            return compressedTaskDescriptor.length;
        }

        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE + (isCompressed() ? compressedTaskDescriptor.length : uncompressedSize);
        }
    }
}
