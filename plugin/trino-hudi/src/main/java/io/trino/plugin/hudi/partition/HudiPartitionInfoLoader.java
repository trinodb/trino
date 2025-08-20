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

public class HudiPartitionInfoLoader
        implements Runnable
{
    private final HudiDirectoryLister hudiDirectoryLister;
    private final HudiSplitFactory hudiSplitFactory;
    private final AsyncQueue<ConnectorSplit> asyncQueue;
    private final Deque<HiveHudiPartitionInfo> partitionQueue;
    private final String commitTime;
    private final boolean useIndex;

    private boolean isRunning;

    public HudiPartitionInfoLoader(
            HudiDirectoryLister hudiDirectoryLister,
            String commitTime,
            HudiSplitFactory hudiSplitFactory,
            AsyncQueue<ConnectorSplit> asyncQueue,
            Deque<HiveHudiPartitionInfo> partitionQueue,
            boolean useIndex)
    {
        this.hudiDirectoryLister = hudiDirectoryLister;
        this.commitTime = commitTime;
        this.hudiSplitFactory = hudiSplitFactory;
        this.asyncQueue = asyncQueue;
        this.partitionQueue = partitionQueue;
        this.isRunning = true;
        this.useIndex = useIndex;
    }

    @Override
    public void run()
    {
        while (isRunning || !partitionQueue.isEmpty()) {
            HiveHudiPartitionInfo hudiPartitionInfo = partitionQueue.poll();

            if (hudiPartitionInfo != null && hudiPartitionInfo.getHivePartitionName() != null) {
                generateSplitsFromPartition(hudiPartitionInfo);
            }
        }
    }

    private void generateSplitsFromPartition(HiveHudiPartitionInfo hudiPartitionInfo)
    {
        List<HivePartitionKey> partitionKeys = hudiPartitionInfo.getHivePartitionKeys();
        List<FileSlice> partitionFileSlices = hudiDirectoryLister.listStatus(hudiPartitionInfo, useIndex);
        partitionFileSlices.stream()
                .flatMap(slice -> hudiSplitFactory.createSplits(partitionKeys, slice, this.commitTime).stream())
                .map(asyncQueue::offer)
                .forEachOrdered(MoreFutures::getFutureValue);
    }

    public void stopRunning()
    {
        this.isRunning = false;
    }
}
