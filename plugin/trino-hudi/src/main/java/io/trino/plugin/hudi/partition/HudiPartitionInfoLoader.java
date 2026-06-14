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
package io.trino.plugin.hudi.partition;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.util.AsyncQueue;
import io.trino.plugin.hive.util.ResumableTask;
import io.trino.plugin.hive.util.ResumableTask.TaskStatus;
import io.trino.plugin.hudi.HudiFileStatus;
import io.trino.plugin.hudi.query.HudiDirectoryLister;
import io.trino.plugin.hudi.split.HudiSplitFactory;
import io.trino.spi.connector.ConnectorSplit;

import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.plugin.hudi.partition.HiveHudiPartitionInfo.NON_PARTITION;

/**
 * HudiPartitionInfoLoader is a ResumableTask that generates splits from Hudi partitions.
 * It uses a non-blocking approach to avoid deadlocks when the split queue is full.
 * The task yields control when waiting for queue space instead of blocking the thread.
 */
public class HudiPartitionInfoLoader
        implements ResumableTask
{
    private static final Logger log = Logger.get(HudiPartitionInfoLoader.class);

    private final HudiDirectoryLister hudiDirectoryLister;
    private final HudiSplitFactory hudiSplitFactory;
    private final AsyncQueue<ConnectorSplit> asyncQueue;
    private final Deque<String> partitionQueue;
    private final Deque<Iterator<ConnectorSplit>> splitIterators;

    private boolean isRunning;

    /**
     * Creates a new split loader.
     *
     * @param hudiDirectoryLister Service for listing files in a partition.
     * @param hudiSplitFactory Factory to generate {@link ConnectorSplit}s.
     * @param asyncQueue The output queue to send generated splits to.
     * @param partitionQueue The input queue of partitions to process.
     * @param splitIterators A deque, private to this worker, used to store
     * partially processed split iterators. This allows the task to save
     * its state when yielding (e.g., when the asyncQueue is full) and
     * resume processing from the same point.
     */
    public HudiPartitionInfoLoader(
            HudiDirectoryLister hudiDirectoryLister,
            HudiSplitFactory hudiSplitFactory,
            AsyncQueue<ConnectorSplit> asyncQueue,
            Deque<String> partitionQueue,
            Deque<Iterator<ConnectorSplit>> splitIterators)
    {
        this.hudiDirectoryLister = hudiDirectoryLister;
        this.hudiSplitFactory = hudiSplitFactory;
        this.asyncQueue = asyncQueue;
        this.partitionQueue = partitionQueue;
        this.splitIterators = splitIterators;
        this.isRunning = true;
    }

    @Override
    public TaskStatus process()
    {
        while (isRunning || (!partitionQueue.isEmpty() || !splitIterators.isEmpty())) {
            try {
                ListenableFuture<Void> future = loadSplits();
                if (!future.isDone()) {
                    return TaskStatus.continueOn(future);
                }
            }
            catch (Exception e) {
                throw new RuntimeException("Error loading splits", e);
            }
        }

        return TaskStatus.finished();
    }

    private ListenableFuture<Void> loadSplits()
    {
        Iterator<ConnectorSplit> splits = splitIterators.poll();
        if (splits == null) {
            String partition = partitionQueue.poll();
            if (partition == null) {
                return immediateVoidFuture();
            }
            splits = generateSplitsFromPartition(partition);
        }

        while (splits.hasNext()) {
            ConnectorSplit split = splits.next();
            ListenableFuture<Void> future = asyncQueue.offer(split);
            if (!future.isDone()) {
                log.debug("AsyncQueue is full, yielding split loader task");
                splitIterators.addFirst(splits);
                return future;
            }
        }

        return immediateVoidFuture();
    }

    private Iterator<ConnectorSplit> generateSplitsFromPartition(String partitionName)
    {
        Optional<HudiPartitionInfo> partitionInfo = hudiDirectoryLister.getPartitionInfo(partitionName);
        if (partitionInfo.isEmpty()) {
            return List.<ConnectorSplit>of().iterator();
        }

        HudiPartitionInfo hudiPartitionInfo = partitionInfo.get();
        if (!hudiPartitionInfo.doesMatchPredicates() && !partitionName.equals(NON_PARTITION)) {
            return List.<ConnectorSplit>of().iterator();
        }

        List<HivePartitionKey> partitionKeys = hudiPartitionInfo.getHivePartitionKeys();
        List<HudiFileStatus> partitionFiles = hudiDirectoryLister.listStatus(hudiPartitionInfo);

        return partitionFiles.stream()
                .flatMap(fileStatus -> hudiSplitFactory.createSplits(partitionKeys, fileStatus).stream())
                .map(ConnectorSplit.class::cast)
                .iterator();
    }

    public void stopRunning()
    {
        this.isRunning = false;
    }
}
