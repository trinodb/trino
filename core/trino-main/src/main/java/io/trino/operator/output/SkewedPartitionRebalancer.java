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
package io.trino.operator.output;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.execution.resourcegroups.IndexedPriorityQueue;
import io.trino.operator.PartitionFunction;
import io.trino.spi.connector.ConnectorBucketNodeMap;
import io.trino.spi.type.Type;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.SystemPartitioningHandle;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.planner.PartitioningHandle.isScaledWriterHashDistribution;
import static java.lang.Double.isNaN;
import static java.lang.Math.ceil;
import static java.lang.Math.floorMod;
import static java.lang.Math.max;

/**
 * Helps in distributing big or skewed partitions across available tasks to improve the performance of
 * partitioned writes.
 * <p>
 * This rebalancer initialize a bunch of buckets for each task based on a given taskBucketCount and then tries to
 * uniformly distribute partitions across those buckets. This helps to mitigate two problems:
 * 1. Mitigate skewness across tasks.
 * 2. Scale few big partitions across tasks even if there's no skewness among them. This will essentially speed the
 *    local scaling without impacting much overall resource utilization.
 * <p>
 * Example:
 * <p>
 * Before: 3 tasks, 3 buckets per task, and 2 skewed partitions
 * Task1                Task2               Task3
 * Bucket1 (Part 1)     Bucket1 (Part 2)    Bucket1
 * Bucket2              Bucket2             Bucket2
 * Bucket3              Bucket3             Bucket3
 * <p>
 * After rebalancing:
 * Task1                Task2               Task3
 * Bucket1 (Part 1)     Bucket1 (Part 2)    Bucket1 (Part 1)
 * Bucket2 (Part 2)     Bucket2 (Part 1)    Bucket2 (Part 2)
 * Bucket3              Bucket3             Bucket3
 */
@ThreadSafe
public class SkewedPartitionRebalancer
{
    private static final Logger log = Logger.get(SkewedPartitionRebalancer.class);
    // Keep the scale writers partition count big enough such that we could rebalance skewed partitions
    // at more granularity, thus leading to less resource utilization at writer stage.
    private static final int SCALE_WRITERS_PARTITION_COUNT = 4096;
    // If the percentage difference between the two different task buckets with maximum and minimum processed bytes
    // since last rebalance is above 0.7 (or 70%), then we consider them skewed.
    private static final double TASK_BUCKET_SKEWNESS_THRESHOLD = 0.7;

    private final int partitionCount;
    private final int taskCount;
    private final int taskBucketCount;
    private final long minPartitionDataProcessedRebalanceThreshold;
    private final long minDataProcessedRebalanceThreshold;

    private final AtomicLongArray partitionRowCount;
    private final AtomicLong dataProcessed;
    private final AtomicLong dataProcessedAtLastRebalance;

    @GuardedBy("this")
    private final long[] partitionDataSize;

    @GuardedBy("this")
    private final long[] partitionDataSizeAtLastRebalance;

    @GuardedBy("this")
    private final long[] partitionDataSizeSinceLastRebalancePerTask;

    @GuardedBy("this")
    private final long[] estimatedTaskBucketDataSizeSinceLastRebalance;

    private final List<List<TaskBucket>> partitionAssignments;

    public static boolean checkCanScalePartitionsRemotely(Session session, int taskCount, PartitioningHandle partitioningHandle, NodePartitioningManager nodePartitioningManager)
    {
        // In case of connector partitioning, check if bucketToPartitions has fixed mapping or not. If it is fixed
        // then we can't distribute a bucket across multiple tasks.
        boolean hasFixedNodeMapping = partitioningHandle.getCatalogHandle()
                .map(handle -> nodePartitioningManager.getConnectorBucketNodeMap(session, partitioningHandle)
                        .map(ConnectorBucketNodeMap::hasFixedMapping)
                        .orElse(false))
                .orElse(false);
        // Use skewed partition rebalancer only when there are more than one tasks
        return taskCount > 1 && !hasFixedNodeMapping && isScaledWriterHashDistribution(partitioningHandle);
    }

    public static PartitionFunction createPartitionFunction(
            Session session,
            NodePartitioningManager nodePartitioningManager,
            PartitioningScheme scheme,
            List<Type> partitionChannelTypes)
    {
        PartitioningHandle handle = scheme.getPartitioning().getHandle();
        // In case of SystemPartitioningHandle we can use arbitrary bucket count so that skewness mitigation
        // is more granular.
        // Whereas, in the case of connector partitioning we have to use connector provided bucketCount
        // otherwise buckets will get mapped to tasks incorrectly which could affect skewness handling.
        //
        // For example: if there are 2 hive buckets, 2 tasks, and 10 artificial bucketCount then this
        // could be how actual hive buckets are mapped to artificial buckets and tasks.
        //
        // hive bucket  artificial bucket   tasks
        // 0            0, 2, 4, 6, 8       0, 0, 0, 0, 0
        // 1            1, 3, 5, 7, 9       1, 1, 1, 1, 1
        //
        // Here rebalancing will happen slowly even if there's a skewness at task 0 or hive bucket 0 because
        // five artificial buckets resemble the first hive bucket. Therefore, these artificial buckets
        // have to write minPartitionDataProcessedRebalanceThreshold before they get scaled to task 1, which is slow
        // compared to only a single hive bucket reaching the min limit.
        int bucketCount = (handle.getConnectorHandle() instanceof SystemPartitioningHandle)
                ? SCALE_WRITERS_PARTITION_COUNT
                : nodePartitioningManager.getBucketNodeMap(session, handle).getBucketCount();
        return nodePartitioningManager.getPartitionFunction(
                session,
                scheme,
                partitionChannelTypes,
                IntStream.range(0, bucketCount).toArray());
    }

    public static SkewedPartitionRebalancer createSkewedPartitionRebalancer(
            int partitionCount,
            int taskCount,
            int taskPartitionedWriterCount,
            long minPartitionDataProcessedRebalanceThreshold,
            long maxDataProcessedRebalanceThreshold)
    {
        // Keep the task bucket count to 50% of total local writers
        int taskBucketCount = (int) ceil(0.5 * taskPartitionedWriterCount);
        return new SkewedPartitionRebalancer(partitionCount, taskCount, taskBucketCount, minPartitionDataProcessedRebalanceThreshold, maxDataProcessedRebalanceThreshold);
    }

    public static int getTaskCount(PartitioningScheme partitioningScheme)
    {
        // Todo: Handle skewness if there are more nodes/tasks than the buckets coming from connector
        // https://github.com/trinodb/trino/issues/17254
        int[] bucketToPartition = partitioningScheme.getBucketToPartition()
                .orElseThrow(() -> new IllegalArgumentException("Bucket to partition must be set before calculating taskCount"));
        // Buckets can be greater than the actual partitions or tasks. Therefore, use max to find the actual taskCount.
        return IntStream.of(bucketToPartition).max().getAsInt() + 1;
    }

    public SkewedPartitionRebalancer(
            int partitionCount,
            int taskCount,
            int taskBucketCount,
            long minPartitionDataProcessedRebalanceThreshold,
            long maxDataProcessedRebalanceThreshold)
    {
        this.partitionCount = partitionCount;
        this.taskCount = taskCount;
        this.taskBucketCount = taskBucketCount;
        this.minPartitionDataProcessedRebalanceThreshold = minPartitionDataProcessedRebalanceThreshold;
        this.minDataProcessedRebalanceThreshold = max(minPartitionDataProcessedRebalanceThreshold, maxDataProcessedRebalanceThreshold);

        this.partitionRowCount = new AtomicLongArray(partitionCount);
        this.dataProcessed = new AtomicLong();
        this.dataProcessedAtLastRebalance = new AtomicLong();

        this.partitionDataSize = new long[partitionCount];
        this.partitionDataSizeAtLastRebalance = new long[partitionCount];
        this.partitionDataSizeSinceLastRebalancePerTask = new long[partitionCount];
        this.estimatedTaskBucketDataSizeSinceLastRebalance = new long[taskCount * taskBucketCount];

        int[] taskBucketIds = new int[taskCount];
        ImmutableList.Builder<List<TaskBucket>> partitionAssignments = ImmutableList.builder();
        for (int partition = 0; partition < partitionCount; partition++) {
            int taskId = partition % taskCount;
            int bucketId = taskBucketIds[taskId]++ % taskBucketCount;
            partitionAssignments.add(new CopyOnWriteArrayList<>(ImmutableList.of(new TaskBucket(taskId, bucketId))));
        }
        this.partitionAssignments = partitionAssignments.build();
    }

    @VisibleForTesting
    List<List<Integer>> getPartitionAssignments()
    {
        ImmutableList.Builder<List<Integer>> assignedTasks = ImmutableList.builder();
        for (List<TaskBucket> partitionAssignment : partitionAssignments) {
            List<Integer> tasks = partitionAssignment.stream()
                    .map(taskBucket -> taskBucket.taskId)
                    .collect(toImmutableList());
            assignedTasks.add(tasks);
        }
        return assignedTasks.build();
    }

    public int getTaskCount()
    {
        return taskCount;
    }

    public int getTaskId(int partitionId, long index)
    {
        List<TaskBucket> taskIds = partitionAssignments.get(partitionId);
        return taskIds.get(floorMod(index, taskIds.size())).taskId;
    }

    public void addDataProcessed(long dataSize)
    {
        dataProcessed.addAndGet(dataSize);
    }

    public void addPartitionRowCount(int partition, long rowCount)
    {
        partitionRowCount.addAndGet(partition, rowCount);
    }

    public void rebalance()
    {
        long currentDataProcessed = dataProcessed.get();
        if (shouldRebalance(currentDataProcessed)) {
            rebalancePartitions(currentDataProcessed);
        }
    }

    private boolean shouldRebalance(long dataProcessed)
    {
        // Rebalance only when total bytes processed since last rebalance is greater than rebalance threshold
        return (dataProcessed - dataProcessedAtLastRebalance.get()) >= minDataProcessedRebalanceThreshold;
    }

    private synchronized void rebalancePartitions(long dataProcessed)
    {
        if (!shouldRebalance(dataProcessed)) {
            return;
        }

        calculatePartitionDataSize(dataProcessed);

        // initialize partitionDataSizeSinceLastRebalancePerTask
        for (int partition = 0; partition < partitionCount; partition++) {
            int totalAssignedTasks = partitionAssignments.get(partition).size();
            partitionDataSizeSinceLastRebalancePerTask[partition] =
                    (partitionDataSize[partition] - partitionDataSizeAtLastRebalance[partition]) / totalAssignedTasks;
        }

        // Initialize taskBucketMaxPartitions
        List<IndexedPriorityQueue<Integer>> taskBucketMaxPartitions = new ArrayList<>(taskCount * taskBucketCount);
        for (int taskId = 0; taskId < taskCount; taskId++) {
            for (int bucketId = 0; bucketId < taskBucketCount; bucketId++) {
                taskBucketMaxPartitions.add(new IndexedPriorityQueue<>());
            }
        }

        for (int partition = 0; partition < partitionCount; partition++) {
            List<TaskBucket> taskAssignments = partitionAssignments.get(partition);
            for (TaskBucket taskBucket : taskAssignments) {
                IndexedPriorityQueue<Integer> queue = taskBucketMaxPartitions.get(taskBucket.id);
                queue.addOrUpdate(partition, partitionDataSizeSinceLastRebalancePerTask[partition]);
            }
        }

        // Initialize maxTaskBuckets and minTaskBuckets
        IndexedPriorityQueue<TaskBucket> maxTaskBuckets = new IndexedPriorityQueue<>();
        IndexedPriorityQueue<TaskBucket> minTaskBuckets = new IndexedPriorityQueue<>();
        for (int taskId = 0; taskId < taskCount; taskId++) {
            for (int bucketId = 0; bucketId < taskBucketCount; bucketId++) {
                TaskBucket taskBucket = new TaskBucket(taskId, bucketId);
                estimatedTaskBucketDataSizeSinceLastRebalance[taskBucket.id] =
                        calculateTaskBucketDataSizeSinceLastRebalance(taskBucketMaxPartitions.get(taskBucket.id));
                maxTaskBuckets.addOrUpdate(taskBucket, estimatedTaskBucketDataSizeSinceLastRebalance[taskBucket.id]);
                minTaskBuckets.addOrUpdate(taskBucket, Long.MAX_VALUE - estimatedTaskBucketDataSizeSinceLastRebalance[taskBucket.id]);
            }
        }

        rebalanceBasedOnTaskBucketSkewness(maxTaskBuckets, minTaskBuckets, taskBucketMaxPartitions);
        dataProcessedAtLastRebalance.set(dataProcessed);
    }

    private void calculatePartitionDataSize(long dataProcessed)
    {
        long totalPartitionRowCount = 0;
        for (int partition = 0; partition < partitionCount; partition++) {
            totalPartitionRowCount += partitionRowCount.get(partition);
        }

        for (int partition = 0; partition < partitionCount; partition++) {
            partitionDataSize[partition] = (partitionRowCount.get(partition) * dataProcessed) / totalPartitionRowCount;
        }
    }

    private long calculateTaskBucketDataSizeSinceLastRebalance(IndexedPriorityQueue<Integer> maxPartitions)
    {
        long estimatedDataSizeSinceLastRebalance = 0;
        for (int partition : maxPartitions) {
            estimatedDataSizeSinceLastRebalance += partitionDataSizeSinceLastRebalancePerTask[partition];
        }
        return estimatedDataSizeSinceLastRebalance;
    }

    private void rebalanceBasedOnTaskBucketSkewness(
            IndexedPriorityQueue<TaskBucket> maxTaskBuckets,
            IndexedPriorityQueue<TaskBucket> minTaskBuckets,
            List<IndexedPriorityQueue<Integer>> taskBucketMaxPartitions)
    {
        List<Integer> scaledPartitions = new ArrayList<>();
        while (true) {
            TaskBucket maxTaskBucket = maxTaskBuckets.poll();
            if (maxTaskBucket == null) {
                break;
            }

            IndexedPriorityQueue<Integer> maxPartitions = taskBucketMaxPartitions.get(maxTaskBucket.id);
            if (maxPartitions.isEmpty()) {
                continue;
            }

            List<TaskBucket> minSkewedTaskBuckets = findSkewedMinTaskBuckets(maxTaskBucket, minTaskBuckets);
            if (minSkewedTaskBuckets.isEmpty()) {
                break;
            }

            while (true) {
                Integer maxPartition = maxPartitions.poll();
                if (maxPartition == null) {
                    break;
                }

                // Rebalance partition only once in a single cycle. Otherwise, rebalancing will happen quite
                // aggressively in the early stage of write, while it is not required. Thus, it can have an impact on
                // output file sizes and resource usage such that produced files can be small and memory usage
                // might be higher.
                if (scaledPartitions.contains(maxPartition)) {
                    continue;
                }

                int totalAssignedTasks = partitionAssignments.get(maxPartition).size();
                if (partitionDataSize[maxPartition] >= (minPartitionDataProcessedRebalanceThreshold * totalAssignedTasks)) {
                    for (TaskBucket minTaskBucket : minSkewedTaskBuckets) {
                        if (rebalancePartition(maxPartition, minTaskBucket, maxTaskBuckets, minTaskBuckets, partitionDataSize[maxPartition])) {
                            scaledPartitions.add(maxPartition);
                            break;
                        }
                    }
                }
                else {
                    break;
                }
            }
        }
    }

    private List<TaskBucket> findSkewedMinTaskBuckets(TaskBucket maxTaskBucket, IndexedPriorityQueue<TaskBucket> minTaskBuckets)
    {
        ImmutableList.Builder<TaskBucket> minSkewedTaskBuckets = ImmutableList.builder();
        for (TaskBucket minTaskBucket : minTaskBuckets) {
            double skewness =
                    ((double) (estimatedTaskBucketDataSizeSinceLastRebalance[maxTaskBucket.id]
                            - estimatedTaskBucketDataSizeSinceLastRebalance[minTaskBucket.id]))
                            / estimatedTaskBucketDataSizeSinceLastRebalance[maxTaskBucket.id];
            if (skewness <= TASK_BUCKET_SKEWNESS_THRESHOLD || isNaN(skewness)) {
                break;
            }
            if (maxTaskBucket.taskId != minTaskBucket.taskId) {
                minSkewedTaskBuckets.add(minTaskBucket);
            }
        }

        return minSkewedTaskBuckets.build();
    }

    private boolean rebalancePartition(
            int partitionId,
            TaskBucket toTaskBucket,
            IndexedPriorityQueue<TaskBucket> maxTasks,
            IndexedPriorityQueue<TaskBucket> minTasks,
            long partitionDataSize)
    {
        List<TaskBucket> assignments = partitionAssignments.get(partitionId);
        if (assignments.stream().anyMatch(taskBucket -> taskBucket.taskId == toTaskBucket.taskId)) {
            return false;
        }
        assignments.add(toTaskBucket);

        // Update the value of partitionDataSizeAtLastRebalance which will get used to calculate
        // partitionDataSizeSinceLastRebalancePerTask in the next rebalancing cycle.
        partitionDataSizeAtLastRebalance[partitionId] = partitionDataSize;

        int newTaskCount = assignments.size();
        int oldTaskCount = newTaskCount - 1;
        // Since a partition is rebalanced from max to min skewed taskBucket, decrease the priority of max
        // taskBucket as well as increase the priority of min taskBucket.
        for (TaskBucket taskBucket : assignments) {
            if (taskBucket.equals(toTaskBucket)) {
                estimatedTaskBucketDataSizeSinceLastRebalance[taskBucket.id] +=
                        (partitionDataSizeSinceLastRebalancePerTask[partitionId] * oldTaskCount) / newTaskCount;
            }
            else {
                estimatedTaskBucketDataSizeSinceLastRebalance[taskBucket.id] -=
                        partitionDataSizeSinceLastRebalancePerTask[partitionId] / newTaskCount;
            }

            maxTasks.addOrUpdate(taskBucket, estimatedTaskBucketDataSizeSinceLastRebalance[taskBucket.id]);
            minTasks.addOrUpdate(taskBucket, Long.MAX_VALUE - estimatedTaskBucketDataSizeSinceLastRebalance[taskBucket.id]);
        }

        log.warn("Rebalanced partition %s to task %s with taskCount %s", partitionId, toTaskBucket.taskId, assignments.size());
        return true;
    }

    private final class TaskBucket
    {
        private final int taskId;
        private final int id;

        private TaskBucket(int taskId, int bucketId)
        {
            this.taskId = taskId;
            // Unique id for this task and bucket
            this.id = (taskId * taskBucketCount) + bucketId;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(id, id);
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
            TaskBucket that = (TaskBucket) o;
            return that.id == id;
        }
    }
}
