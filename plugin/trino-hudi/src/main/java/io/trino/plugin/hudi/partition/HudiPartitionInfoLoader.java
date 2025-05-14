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

import io.airlift.concurrent.MoreFutures;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.util.AsyncQueue;
import io.trino.plugin.hudi.query.HudiDirectoryLister;
import io.trino.plugin.hudi.split.HudiSplitFactory;
import io.trino.spi.connector.ConnectorSplit;
import org.apache.hudi.common.model.FileSlice;

import java.util.Deque;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.hudi.partition.HiveHudiPartitionInfo.NON_PARTITION;
import static java.util.Objects.requireNonNull;

public class HudiPartitionInfoLoader
        implements Runnable
{
    private final HudiDirectoryLister hudiDirectoryLister;
    private final HudiSplitFactory hudiSplitFactory;
    private final AsyncQueue<ConnectorSplit> asyncQueue;
    private final Deque<String> partitionQueue;
    private final String commitTime;

    private boolean isRunning;

    public HudiPartitionInfoLoader(
            HudiDirectoryLister hudiDirectoryLister,
            HudiSplitFactory hudiSplitFactory,
            AsyncQueue<ConnectorSplit> asyncQueue,
            Deque<String> partitionQueue,
            String commitTime)
    {
        this.hudiDirectoryLister = requireNonNull(hudiDirectoryLister, "hudiDirectoryLister is null");
        this.hudiSplitFactory = requireNonNull(hudiSplitFactory, "hudiSplitFactory is null");
        this.asyncQueue = requireNonNull(asyncQueue, "asyncQueue is null");
        this.partitionQueue = requireNonNull(partitionQueue, "partitionQueue is null");
        this.isRunning = true;
        this.commitTime = requireNonNull(commitTime, "commitTime is null");
    }

    @Override
    public void run()
    {
        while (isRunning || !partitionQueue.isEmpty()) {
            String partitionName = partitionQueue.poll();

            if (partitionName != null) {
                generateSplitsFromPartition(partitionName);
            }
        }
    }

    private void generateSplitsFromPartition(String partitionName)
    {
        Optional<HudiPartitionInfo> partitionInfo = hudiDirectoryLister.getPartitionInfo(partitionName);
        partitionInfo.ifPresent(hudiPartitionInfo -> {
            if (hudiPartitionInfo.doesMatchPredicates() || partitionName.equals(NON_PARTITION)) {
                List<HivePartitionKey> partitionKeys = hudiPartitionInfo.getHivePartitionKeys();
                List<FileSlice> partitionFileSlices = hudiDirectoryLister.listStatus(hudiPartitionInfo, commitTime);
                partitionFileSlices.stream()
                        .flatMap(slice -> hudiSplitFactory.createSplits(partitionKeys, slice, commitTime).stream())
                        .map(asyncQueue::offer)
                        .forEachOrdered(MoreFutures::getFutureValue);
            }
        });
    }

    public void stopRunning()
    {
        this.isRunning = false;
    }
}
