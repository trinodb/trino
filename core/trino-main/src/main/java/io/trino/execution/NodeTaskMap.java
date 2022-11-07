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
package io.trino.execution;

import com.google.common.collect.Sets;
import io.airlift.log.Logger;
import io.trino.metadata.InternalNode;
import io.trino.util.FinalizerService;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class NodeTaskMap
{
    private static final Logger log = Logger.get(NodeTaskMap.class);
    private final ConcurrentHashMap<InternalNode, NodeTasks> nodeTasksMap = new ConcurrentHashMap<>();
    private final FinalizerService finalizerService;

    @Inject
    public NodeTaskMap(FinalizerService finalizerService)
    {
        this.finalizerService = requireNonNull(finalizerService, "finalizerService is null");
    }

    public void addTask(InternalNode node, RemoteTask task)
    {
        createOrGetNodeTasks(node).addTask(task);
    }

    public PartitionedSplitsInfo getPartitionedSplitsOnNode(InternalNode node)
    {
        return createOrGetNodeTasks(node).getPartitionedSplitsInfo();
    }

    public PartitionedSplitCountTracker createPartitionedSplitCountTracker(InternalNode node, TaskId taskId)
    {
        return createOrGetNodeTasks(node).createPartitionedSplitCountTracker(taskId);
    }

    private NodeTasks createOrGetNodeTasks(InternalNode node)
    {
        NodeTasks nodeTasks = nodeTasksMap.get(node);
        if (nodeTasks == null) {
            nodeTasks = addNodeTask(node);
        }
        return nodeTasks;
    }

    private NodeTasks addNodeTask(InternalNode node)
    {
        NodeTasks newNodeTasks = new NodeTasks(finalizerService);
        NodeTasks nodeTasks = nodeTasksMap.putIfAbsent(node, newNodeTasks);
        if (nodeTasks == null) {
            return newNodeTasks;
        }
        return nodeTasks;
    }

    private static class NodeTasks
    {
        private final Set<RemoteTask> remoteTasks = Sets.newConcurrentHashSet();
        private final AtomicInteger nodeTotalPartitionedSplitCount = new AtomicInteger();
        private final AtomicLong nodeTotalPartitionedSplitWeight = new AtomicLong();
        private final FinalizerService finalizerService;

        public NodeTasks(FinalizerService finalizerService)
        {
            this.finalizerService = requireNonNull(finalizerService, "finalizerService is null");
        }

        private PartitionedSplitsInfo getPartitionedSplitsInfo()
        {
            return PartitionedSplitsInfo.forSplitCountAndWeightSum(nodeTotalPartitionedSplitCount.get(), nodeTotalPartitionedSplitWeight.get());
        }

        private void addTask(RemoteTask task)
        {
            if (remoteTasks.add(task)) {
                // Check if task state is already done before adding the listener
                if (task.getTaskStatus().getState().isDone()) {
                    remoteTasks.remove(task);
                    return;
                }

                task.addStateChangeListener(taskStatus -> {
                    if (taskStatus.getState().isDone()) {
                        remoteTasks.remove(task);
                    }
                });
            }
        }

        public PartitionedSplitCountTracker createPartitionedSplitCountTracker(TaskId taskId)
        {
            requireNonNull(taskId, "taskId is null");

            TaskPartitionedSplitCountTracker tracker = new TaskPartitionedSplitCountTracker(taskId, nodeTotalPartitionedSplitCount, nodeTotalPartitionedSplitWeight);
            PartitionedSplitCountTracker partitionedSplitCountTracker = new PartitionedSplitCountTracker(tracker);

            // when partitionedSplitCountTracker is garbage collected, run the cleanup method on the tracker
            // Note: tracker cannot have a reference to partitionedSplitCountTracker
            finalizerService.addFinalizer(partitionedSplitCountTracker, tracker::cleanup);

            return partitionedSplitCountTracker;
        }

        @ThreadSafe
        private static class TaskPartitionedSplitCountTracker
                implements Consumer<PartitionedSplitsInfo>
        {
            private final TaskId taskId;
            private final AtomicInteger nodeTotalPartitionedSplitCount;
            private final AtomicLong nodeTotalPartitionedSplitWeight;
            private final AtomicInteger localPartitionedSplitCount = new AtomicInteger();
            private final AtomicLong localPartitionedSplitWeight = new AtomicLong();

            public TaskPartitionedSplitCountTracker(TaskId taskId, AtomicInteger nodeTotalPartitionedSplitCount, AtomicLong nodeTotalPartitionedSplitWeight)
            {
                this.taskId = requireNonNull(taskId, "taskId is null");
                this.nodeTotalPartitionedSplitCount = requireNonNull(nodeTotalPartitionedSplitCount, "nodeTotalPartitionedSplitCount is null");
                this.nodeTotalPartitionedSplitWeight = requireNonNull(nodeTotalPartitionedSplitWeight, "nodeTotalPartitionedSplitWeight is null");
            }

            @Override
            public synchronized void accept(PartitionedSplitsInfo partitionedSplits)
            {
                if (partitionedSplits == null || partitionedSplits.getCount() < 0 || partitionedSplits.getWeightSum() < 0) {
                    clearLocalSplitInfo(false);
                    requireNonNull(partitionedSplits, "partitionedSplits is null"); // throw NPE if null, otherwise negative value
                    throw new IllegalArgumentException("Invalid negative value: " + partitionedSplits);
                }

                int newCount = partitionedSplits.getCount();
                long newWeight = partitionedSplits.getWeightSum();
                int countDelta = newCount - localPartitionedSplitCount.getAndSet(newCount);
                long weightDelta = newWeight - localPartitionedSplitWeight.getAndSet(newWeight);
                if (countDelta != 0) {
                    nodeTotalPartitionedSplitCount.addAndGet(countDelta);
                }
                if (weightDelta != 0) {
                    nodeTotalPartitionedSplitWeight.addAndGet(weightDelta);
                }
            }

            private void clearLocalSplitInfo(boolean reportAsLeaked)
            {
                int leakedCount = localPartitionedSplitCount.getAndSet(0);
                long leakedWeight = localPartitionedSplitWeight.getAndSet(0);
                if (leakedCount == 0 && leakedWeight == 0) {
                    return;
                }

                if (reportAsLeaked) {
                    log.error("BUG! %s for %s leaked with %s partitioned splits (weight: %s). Cleaning up so server can continue to function.",
                            getClass().getName(),
                            taskId,
                            leakedCount,
                            leakedWeight);
                }

                nodeTotalPartitionedSplitCount.addAndGet(-leakedCount);
                nodeTotalPartitionedSplitWeight.addAndGet(-leakedWeight);
            }

            public void cleanup()
            {
                clearLocalSplitInfo(true);
            }

            @Override
            public String toString()
            {
                return toStringHelper(this)
                        .add("taskId", taskId)
                        .add("splits", localPartitionedSplitCount)
                        .add("weight", localPartitionedSplitWeight)
                        .toString();
            }
        }
    }

    public static class PartitionedSplitCountTracker
    {
        private final Consumer<PartitionedSplitsInfo> splitSetter;

        public PartitionedSplitCountTracker(Consumer<PartitionedSplitsInfo> splitSetter)
        {
            this.splitSetter = requireNonNull(splitSetter, "splitSetter is null");
        }

        public void setPartitionedSplits(PartitionedSplitsInfo partitionedSplits)
        {
            splitSetter.accept(partitionedSplits);
        }

        @Override
        public String toString()
        {
            return splitSetter.toString();
        }
    }
}
